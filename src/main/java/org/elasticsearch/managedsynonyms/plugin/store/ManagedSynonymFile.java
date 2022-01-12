package org.elasticsearch.managedsynonyms.plugin.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.managedsynonyms.plugin.ManagedSynonymException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.stream.Collectors;

public class ManagedSynonymFile {
    private static final Logger logger = LogManager.getLogger(ManagedSynonymFile.class);

    private final String name;
    private long lastSyncTimestamp;

    private Map<String, ManagedSynonymSet> synonymSetIdMap = new HashMap<String, ManagedSynonymSet>();
    private Map<String, ManagedSynonymSet> synonymSetTermMap = new HashMap<String, ManagedSynonymSet>();
    private Set<String> updatedSetIds = new HashSet<String>();
    private Set<String> deletedSetIds = new HashSet<String>();
    private StampedLock setLock = new StampedLock();

    public ManagedSynonymFile(String name) {
        this.name = name;
        lastSyncTimestamp = 0;
    }

    public String getName() {
        return this.name;
    }

    public long getLastSyncTimestamp() {
        return lastSyncTimestamp;
    }

    public ManagedSynonymSet getSet(String setId) {
        var stamp = setLock.readLock();
        try {
            if (synonymSetIdMap.containsKey(setId) == false) return null;
            return synonymSetIdMap.get(setId);
        } finally {
            setLock.unlock(stamp);
        }
    }

    public int getCount() {
        return getCount(null);
    }

    public List<ManagedSynonymSet> getAll() {
        var stamp = setLock.readLock();
        try {
            return synonymSetIdMap.entrySet().stream().map(p -> p.getValue()).collect(Collectors.toList());
        } finally {
            setLock.unlock(stamp);
        }
    }

    private boolean setHasQuery(String query, ManagedSynonymSet set) {
        return query == null || query.length() == 0 || set.hasItem(query);
    }

    public int getCount(String query) {
        var stamp = setLock.readLock();
        try {
            return synonymSetIdMap.values().stream().filter(p -> setHasQuery(query, p)).collect(Collectors.toList()).size();
        } finally {
            setLock.unlock(stamp);
        }
    }

    public List<ManagedSynonymSet> listSets(int page, int itemsPerPage, String query) {
        int startIndex = (page - 1) * itemsPerPage;

        List<ManagedSynonymSet> ret = null;
        var stamp = setLock.readLock();
        try {
            if (startIndex >= synonymSetIdMap.size()) return new ArrayList<ManagedSynonymSet>();
            ret = synonymSetIdMap.values().stream().filter(p -> setHasQuery(query, p)).collect(Collectors.toList());
        } finally {
            setLock.unlock(stamp);
        }

        if (ret == null) {
            return ret;
        }

        Collections.sort(ret);
        var limited = ret.stream().skip(startIndex).limit(itemsPerPage).collect(Collectors.toList());
        return limited;
    }

    public ManagedSynonymSet createSynonymSet(List<String> terms) throws ManagedSynonymException {
        var newSetId = java.util.UUID.randomUUID().toString().toLowerCase();
        var newSet = new ManagedSynonymSet(newSetId, terms);

        var stamp = setLock.readLock();
        try {
            var existingTerms = findAnyExistingTerms(newSet);

            if (existingTerms.size() > 0) {
                throw new ManagedSynonymException(
                    String.format("The following terms already exist in a synonym set: %s", String.join(", ", existingTerms))
                );
            }

            stamp = setLock.tryConvertToWriteLock(stamp);
            addSynonymTermMapping(newSet);
            return newSet;
        } finally {
            setLock.unlock(stamp);
        }
    }

    public ManagedSynonymSet updateSynonymSet(ManagedSynonymSet set) throws ManagedSynonymException {
        var stamp = setLock.readLock();
        try {
            if (synonymSetIdMap.containsKey(set.getId()) == false) return null;

            ManagedSynonymSet foundSet = synonymSetIdMap.get(set.getId());

            var existingTerms = findAnyExistingTerms(set, foundSet);

            if (existingTerms.size() > 0) {
                throw new ManagedSynonymException(
                    String.format("The following terms already exist in a synonym set: %s", String.join(", ", existingTerms))
                );
            }

            stamp = setLock.tryConvertToWriteLock(stamp);
            removeSynonymMapping(foundSet);
            addSynonymTermMapping(set);
            return set;
        } finally {
            setLock.unlock(stamp);
        }
    }

    public boolean deleteSynonymSet(String setId) {
        var stamp = setLock.readLock();
        try {
            if (synonymSetIdMap.containsKey(setId) == false) return false;

            ManagedSynonymSet foundSet = synonymSetIdMap.get(setId);
            stamp = setLock.tryConvertToWriteLock(stamp);
            removeSynonymMapping(foundSet);
            return true;
        } finally {
            setLock.unlock(stamp);
        }
    }

    public void syncItems(List<ManagedSynonymSet> sets, boolean clearItems) {
        var stamp = setLock.writeLock();
        try {
            if (clearItems) this.clearAll();
            for (ManagedSynonymSet set : sets) {
                var newSet = new ManagedSynonymSet(set);
                this.addSynonymTermMapping(newSet, false);
            }
        } finally {
            setLock.unlock(stamp);
        }
    }

    public Map<String, Object> toSettingsMap() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put("name", this.name);

        var dataMap = new HashMap<String, Object>();
        var stamp = setLock.readLock();
        try {
            for (var set : this.synonymSetIdMap.values()) {
                dataMap.put(set.getId(), set.toSettingsMap());
            }
            return settingsMap;
        } finally {
            setLock.unlock(stamp);
        }
    }

    public static ManagedSynonymFile fromSettings(Settings settings) throws ManagedSynonymException {
        var fileName = settings.get("name");
        var sets = settings.getAsSettings("data");

        var newFile = new ManagedSynonymFile(fileName);
        for (var key : sets.keySet()) {
            var thisSet = ManagedSynonymSet.fromSettings(sets.getAsSettings(key));
            newFile.addSynonymTermMapping(thisSet);
        }
        return newFile;
    }

    public List<ManagedSynonymSet> getUpdatedSets() {
        var ret = new ArrayList<ManagedSynonymSet>();
        var stamp = setLock.readLock();
        try {
            for (var setId : this.updatedSetIds) {
                var thisSet = this.synonymSetIdMap.get(setId);
                if (thisSet != null) ret.add(thisSet);
            }
        } finally {
            setLock.unlock(stamp);
        }
        return ret;
    }

    public List<String> getDeletedSets() {
        var stamp = setLock.readLock();
        try {
            return new ArrayList<String>(this.deletedSetIds);
        } finally {
            setLock.unlock(stamp);
        }
    }

    /* ===== */

    private void addSynonymTermMapping(ManagedSynonymSet set) {
        addSynonymTermMapping(set, true);
    }

    private void addSynonymTermMapping(ManagedSynonymSet set, boolean setDirtyFlag) {
        synonymSetIdMap.put(set.getId(), set);
        List<String> setItems = set.getItems();
        for (String term : setItems) {
            synonymSetTermMap.put(term, set);
        }
        if (setDirtyFlag) {
            updatedSetIds.add(set.getId());
            deletedSetIds.remove(set.getId());
        }
    }

    private void removeSynonymMapping(ManagedSynonymSet set) {
        synonymSetIdMap.remove(set.getId());
        for (String term : set.getItems()) {
            synonymSetTermMap.remove(term);
        }
        deletedSetIds.add(set.getId());
        updatedSetIds.remove(set.getId());
    }

    private List<String> findAnyExistingTerms(ManagedSynonymSet set) {
        return findAnyExistingTerms(set, null);
    }

    private List<String> findAnyExistingTerms(ManagedSynonymSet set, ManagedSynonymSet ignoreSet) {
        var existingTerms = new ArrayList<String>();
        for (String term : set.getItems()) {
            var mappedSet = synonymSetTermMap.get(term);
            if (mappedSet == null) continue;

            if (ignoreSet != null && ignoreSet == mappedSet) continue;

            existingTerms.add(term);
        }
        return existingTerms;
    }

    private void clearAll() {
        synonymSetIdMap.clear();
        synonymSetTermMap.clear();
        updatedSetIds.clear();
        deletedSetIds.clear();
    }

}
