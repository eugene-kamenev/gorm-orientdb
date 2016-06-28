package grails.gorm.tests.orientdb.graph

import grails.gorm.tests.GormDatastoreSpec
import org.grails.datastore.gorm.orient.entity.graph.TestEntity

class OrientDbFindWhereSpec extends GormDatastoreSpec {
    def "Test findWhere returns a matching Instance"() {
        given:
        def entityId = new TestEntity(name: 'David', age: 27).save().id


        when:
        def entity = TestEntity.findWhere(name: 'David')

        then:
        'David' == entity.name
        27 == entity.age
        entityId == entity.id
    }

    def "Test findWhere with a GString property"() {
        given:
        def entityId = new TestEntity(name: 'David', age: 27).save().id
        def property = "name"

        when:
        def entity = TestEntity.findWhere("${property}": 'David')

        then:
        'David' == entity.name
        27 == entity.age
        entityId == entity.id
    }

    def "Test findAllWhere returns a matching Instance"() {
        given:
        def entityId = new TestEntity(name: 'David', age: 27).save().id


        when:
        def entity = TestEntity.findAllWhere(name: 'David')
        then:
        'David' == entity[0].name
        27 == entity[0].age
        entityId == entity[0].id
    }

    def "Test findAllWhere with a GString property"() {
        given:
        def entityId = new TestEntity(name: 'David', age: 27).save().id
        def property = "name"

        when:
        def entity = TestEntity.findAllWhere("${property}": 'David')

        then:
        'David' == entity[0].name
        27 == entity[0].age
        entityId == entity[0].id
    }
}
