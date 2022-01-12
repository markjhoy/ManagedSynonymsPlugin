package org.elasticsearch.managedsynonyms.plugin.store;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.managedsynonyms.plugin.ManagedSynonymException;
import org.elasticsearch.managedsynonyms.plugin.ManagedSynonymTokenHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ManagedSynonymSet implements Comparable<ManagedSynonymSet> {
    private final String setId;
    private List<String> synonyms = new ArrayList<String>();
    private long createdTimestamp;
    private long updatedTimestamp;

    public ManagedSynonymSet(String setId) {
        this.setId = setId;
        this.createdTimestamp = System.currentTimeMillis();
        this.updatedTimestamp = System.currentTimeMillis();
    }

    public ManagedSynonymSet(ManagedSynonymSet other) {
        this.setId = other.setId;
        this.synonyms.addAll(other.synonyms);
        this.createdTimestamp = other.createdTimestamp;
        this.updatedTimestamp = other.updatedTimestamp;
    }

    public ManagedSynonymSet(String setId, Collection<String> listToSet) {
        this.setId = setId;
        this.setList(listToSet);
        this.createdTimestamp = System.currentTimeMillis();
        this.updatedTimestamp = System.currentTimeMillis();
    }

    public ManagedSynonymSet(String setId, Collection<String> listToSet, long createdTime, long updatedTime) {
        this.setId = setId;
        this.setList(listToSet);
        this.createdTimestamp = createdTime;
        this.updatedTimestamp = updatedTime;
    }

    public long getCreatedTimestamp() {
        return this.createdTimestamp;
    }

    public long getUpdatedTimestamp() {
        return this.updatedTimestamp;
    }

    public String getId() {
        return this.setId;
    }

    public void setList(Collection<String> listToSet) {
        synonyms.clear();
        updatedTimestamp = System.currentTimeMillis();
        for (String term : listToSet) {
            addItem(term);
        }
    }

    public boolean addItem(String newItem) {
        String normalizedSynonym = ManagedSynonymTokenHelper.normalize(newItem);
        if (synonyms.contains(normalizedSynonym)) {
            return false;
        }
        synonyms.add(normalizedSynonym);
        updatedTimestamp = System.currentTimeMillis();
        return true;
    }

    public void clear() {
        synonyms.clear();
        updatedTimestamp = System.currentTimeMillis();
    }

    public boolean hasItem(String newItem) {
        if (newItem == null) return false;
        String normalizedSynonym = ManagedSynonymTokenHelper.normalize(newItem);
        return synonyms.contains(normalizedSynonym);
    }

    public int size() {
        return synonyms.size();
    }

    public List<String> getItems() {
        return new ArrayList<String>(synonyms);
    }

    public String synonymsToString() {
        return String.join(",", synonyms);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", this.setId);
        builder.field("createdTimestamp", this.createdTimestamp);
        builder.field("updatedTimestamp", this.updatedTimestamp);
        builder.array("synonyms", this.synonyms);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return this.setId + "," + this.synonymsToString();
    }

    @Override
    public int compareTo(ManagedSynonymSet other) {
        // by updated timestamp descending
        return this.updatedTimestamp > other.updatedTimestamp ? -1 : this.updatedTimestamp == other.updatedTimestamp ? 0 : 1;
    }

    public Map<String, Object> toSettingsMap() {
        var settingsMap = new HashMap<String, Object>();
        settingsMap.put("setId", this.setId);
        settingsMap.put("synonyms", this.synonyms);
        settingsMap.put("created", this.createdTimestamp);
        settingsMap.put("updated", this.updatedTimestamp);
        return settingsMap;
    }

    public static ManagedSynonymSet fromSettings(Settings settings) throws ManagedSynonymException {
        var setId = settings.get("setId");
        var synonyms = settings.getAsList("synonyms");
        var created = settings.getAsLong("created", 0L);
        var updated = settings.getAsLong("updated", 0L);
        if (setId == null || synonyms == null || synonyms.size() == 0 || created == 0L || updated == 0L) throw new ManagedSynonymException(
            "Could not deserialize synonym set settings"
        );
        return new ManagedSynonymSet(setId, synonyms, created, updated);
    }
}
