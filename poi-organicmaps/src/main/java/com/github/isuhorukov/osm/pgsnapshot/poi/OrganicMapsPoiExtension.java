package com.github.isuhorukov.osm.pgsnapshot.poi;

import com.beust.jcommander.Parameter;
import com.github.isuhorukov.osm.pgsnapshot.extension.PostgresqlResources;
import com.github.isuhorukov.osm.pgsnapshot.extension.TagExtension;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifier.POI_CATEGORY_TAG;
import static com.github.isuhorukov.osm.pgsnapshot.poi.OrganicMapsPoiClassifier.POI_TAG;

public class OrganicMapsPoiExtension implements TagExtension {

    @Parameter(names = {"-poi_mapping_dir"}, description = "Organic Maps 'data' directory (mapcss-mapping.csv, replaced_tags.txt, categories.txt) to add POI type tags is.poi and is.poi_category")
    private String poiMappingDir;

    private OrganicMapsPoiClassifier classifier;

    @Override
    public String getName() {
        return "organicmaps-poi";
    }

    @Override
    public Object getParameters() {
        return this;
    }

    @Override
    public boolean isEnabled() {
        return poiMappingDir != null;
    }

    @Override
    public void init() throws IOException {
        classifier = OrganicMapsPoiClassifier.load(Paths.get(poiMappingDir));
    }

    @Override
    public List<Tag> additionalTags(Entity entity) {
        if (classifier == null) {
            throw new IllegalStateException("init() was not called");
        }
        return classifier.poiTags(entity.getTags());
    }

    @Override
    public Set<String> multipolygonTagKeys() {
        return Set.of(POI_TAG, POI_CATEGORY_TAG);
    }

    @Override
    public void writePostgresqlResources(PostgresqlResources resources) throws IOException {
        PoiTypeCatalogWriter.write(resources, classifier.getPoiTypes());
    }
}
