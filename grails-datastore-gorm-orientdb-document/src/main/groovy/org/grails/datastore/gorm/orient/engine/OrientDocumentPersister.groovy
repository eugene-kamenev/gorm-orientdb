package org.grails.datastore.gorm.orient.engine

import com.orientechnologies.orient.core.db.record.OIdentifiable
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.grails.datastore.mapping.core.Session
import org.grails.datastore.mapping.engine.AssociationIndexer
import org.grails.datastore.mapping.engine.EntityAccess
import org.grails.datastore.mapping.engine.NativeEntryEntityPersister
import org.grails.datastore.mapping.engine.PropertyValueIndexer
import org.grails.datastore.mapping.model.MappingContext
import org.grails.datastore.mapping.model.PersistentEntity
import org.grails.datastore.mapping.model.PersistentProperty
import org.grails.datastore.mapping.model.types.Association
import org.grails.datastore.mapping.query.Query
import org.springframework.context.ApplicationEventPublisher

/**
 * OrientDB entity persister class
 *
 * @author eugene.kamenev
 */
@CompileStatic
@Slf4j
class OrientDocumentPersister extends NativeEntryEntityPersister<OIdentifiable, Object> {

    OrientDocumentPersister(MappingContext mappingContext, PersistentEntity entity, Session session, ApplicationEventPublisher publisher) {
        super(mappingContext, entity, session, publisher)
    }

    /**
     * Return document collection name
     * @return
     */
    @Override
    String getEntityFamily() {
        return null
    }

    /**
     * Delete single document
     *
     * @param family
     * @param key
     * @param entry
     */
    @Override
    protected void deleteEntry(String family, Object key, Object entry) {

    }

    /**
     * Generate new identifier for entry
     *
     * @param persistentEntity
     * @param entry
     * @return
     */
    @Override
    protected Object generateIdentifier(PersistentEntity persistentEntity, OIdentifiable entry) {
        return null
    }

    /**
     * Get property indexer method
     *
     * @param property
     * @return
     */
    @Override
    PropertyValueIndexer getPropertyIndexer(PersistentProperty property) {
        return null
    }

    /**
     * Get association indexer
     *
     * @param nativeEntry
     * @param association
     * @return
     */
    @Override
    AssociationIndexer getAssociationIndexer(OIdentifiable nativeEntry, Association association) {
        return null
    }

    /**
     * Create new document
     *
     * @param family
     * @return
     */
    @Override
    protected OIdentifiable createNewEntry(String family) {
        return null
    }

    /**
     * Get value for specific field from document
     *
     * @param nativeEntry
     * @param property
     * @return
     */
    @Override
    protected Object getEntryValue(OIdentifiable nativeEntry, String property) {
        return null
    }

    /**
     * Set value for specific field into document
     *
     * @param nativeEntry
     * @param key
     * @param value
     */
    @Override
    protected void setEntryValue(OIdentifiable nativeEntry, String key, Object value) {

    }

    /**
     * Get entity by identity
     *
     * @param persistentEntity
     * @param family
     * @param key
     * @return
     */
    @Override
    protected OIdentifiable retrieveEntry(PersistentEntity persistentEntity, String family, Serializable key) {
        return null
    }

    /**
     * Persist document
     *
     * @param persistentEntity
     * @param entityAccess
     * @param storeId
     * @param nativeEntry
     * @return
     */
    @Override
    protected Object storeEntry(PersistentEntity persistentEntity, EntityAccess entityAccess, Object storeId, OIdentifiable nativeEntry) {
        return null
    }

    /**
     * Update document
     *
     * @param persistentEntity
     * @param entityAccess
     * @param key
     * @param entry
     */
    @Override
    protected void updateEntry(PersistentEntity persistentEntity, EntityAccess entityAccess, Object key, OIdentifiable entry) {

    }

    /**
     * Delete documents
     *
     * @param family
     * @param keys
     */
    @Override
    protected void deleteEntries(String family, List<Object> keys) {

    }

    /**
     * Create GORM query
     *
     * @return
     */
    @Override
    Query createQuery() {
        return null
    }
}
