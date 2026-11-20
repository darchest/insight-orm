/*
 * Copyright 2021-2024, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.orm

import org.darchest.insight.*
import org.darchest.insight.dml.Delete
import org.darchest.insight.dml.Insert
import org.darchest.insight.dml.Update
import org.darchest.insight.impl.ReadableSelectImp
import org.darchest.insight.impl.select
import java.sql.Connection

interface Entity<Key: Any, T: Table> {

    val me: T
    var cursor: ReadableSelect<T>?

    suspend fun count(where: T.() -> SqlValue<*, *>): Long {
        return count(where.invoke(me))
    }

    suspend fun count(where: SqlValue<*, *>? = null): Long {
        val cnt = me.vendor().getCountExpression()
        read(arrayOf(cnt), where, null, null, null, true)
        next()
        return cnt()
    }

    suspend fun findOne(where: T.() -> SqlValue<*, *>): Key? {
        val keys = find(where.invoke(me))
        return if (keys.isNotEmpty()) keys[0] else null
    }

    suspend fun findOne(where: SqlValue<*, *>? = null): Key? {
        val keys = find(where)
        return if (keys.isNotEmpty()) keys[0] else null
    }

    suspend fun find(where: T.() -> SqlValue<*, *>): List<Key> {
        return find(where.invoke(me))
    }

    suspend fun find(where: SqlValue<*, *>? = null): List<Key> {
        val idKey = getIdCol(me)
        val keys = mutableListOf<Key>()
        read(arrayOf(idKey), where)
        while (next())
            keys.add(idKey())
        return keys
    }

    suspend fun beforeRead(select: ReadableSelect<T>) {

    }

    suspend fun beforeCreate(t: T, id: Key) {

    }

    suspend fun afterCreate(t: T, id: Key) {

    }

    /** Called with the prepared copy and the source record ID. */
    suspend fun beforeCopy(t: T, sourceId: Key) {

    }

    fun fieldsToCheck(t: T): MutableList<SqlValue<*, *>> = mutableListOf(getIdCol(t))

    suspend fun beforeUpdate(t: T, id: Key, srv: T) {

    }

    suspend fun beforeRemove(id: Key, srv: T) {

    }

    suspend fun read(fs: Array<SqlValue<*, *>>, where: SqlValue<*, *>? = null, sort: Array<SortInfo>? = null, limit: Long? = null, offset: Long? = null, isCount: Boolean = false, connection: Connection? = null) {
        val sel = ReadableSelectImp(me)
        sel.connection = connection
        sel.fields(*fs)
        sel.where(where)
        if (sort != null)
            sel.sort(*sort)
        sel.limit(limit)
        sel.offset(offset)
        beforeRead(sel)
        sel.read()
        cursor = sel
    }

    suspend fun groupBy(groupFields: Collection<SqlValue<*, *>>, fields: Collection<SqlValue<*, *>>, where: SqlValue<*, *>? = null, sort: Array<SortInfo>? = null, limit: Long? = null, offset: Long? = null, isCount: Boolean = false, connection: Connection? = null) {
        val sel = ReadableSelectImp(me)
        sel.connection = connection
        sel.groupBy(groupFields)
        sel.fields(fields)
        sel.where(where)
        if (sort != null)
            sel.sort(*sort)
        sel.limit(limit)
        sel.offset(offset)
        beforeRead(sel)
        sel.read()
        cursor = sel
    }

    fun next() = cursor!!.next()

    suspend fun createIfNotExist(id: Key) {
        val has = count(eqIdExpr(me, id)) != 0L
        if (!has)
            create(id)
    }

    suspend fun createOrUpdate(id: Key) {
        val has = count(eqIdExpr(me, id)) != 0L
        if (has)
            update(id)
        else
            create(id)
    }

    suspend fun create(): Key {
        return create(generateKey())
    }

    suspend fun create(id: Key): Key {
        setupId(me, id)
        beforeCreate(me, id)
        Insert()
            .addRows(me)
            .execute()
        afterCreate(me, id)
        return id
    }

    suspend fun copy(id: Key): Key {
        val columns = me.columns()
        require(columns.none { it.state == SqlValue.State.EXPR_SET }) { "Copy requires concrete column values" }
        val keyColumn = getIdCol(me)
        val newId = if (keyColumn.state == SqlValue.State.VALUE_SET) {
            requireNotNull(keyColumn.getValue()) { "The copy's primary key must have a value" }
        } else null

        val targetColumns = columns.filter { it.name != keyColumn.name && it.state != SqlValue.State.VALUE_SET }
        val source = tableFactory()
        val sourceColumns = targetColumns.map { source.column(it.name) }
        val sel = select(source) {
            fields(sourceColumns.ifEmpty { listOf(getIdCol(source)) })
            where(eqIdExpr(source, id))
            limit(1)
        }
        beforeRead(sel)
        sel.read()
        if (!sel.next()) throw RuntimeException("Record not found to copy $id")

        val sourceValues = sourceColumns.map { it.getValue() }
        for ((index, target) in targetColumns.withIndex()) {
            target.state = SqlValue.State.NOT_SET
            @Suppress("UNCHECKED_CAST")
            (target as TableColumn<Any, *>)(sourceValues[index])
        }
        if (keyColumn.state == SqlValue.State.READED) keyColumn.state = SqlValue.State.NOT_SET
        beforeCopy(me, id)
        return if (newId == null) create() else create(newId)
    }

    suspend fun update(id: Key): Int {
        val srv = tableFactory()
        val sel = select(srv) {
            fields(fieldsToCheck(srv))
            where(eqIdExpr(srv, id))
            limit(1)
        }
        sel.read()
        if (!sel.next())
            throw RuntimeException("Record not found to update $id")
        beforeUpdate(me, id, srv)
        return Update(me)
            .where(eqIdExpr(me, id))
            .execute()
    }

    suspend fun remove(id: Key): Int {
        val srv = tableFactory()
        val sel = select(srv) {
            fields(fieldsToCheck(srv))
            where(eqIdExpr(srv, id))
            limit(1)
        }
        sel.read()
        if (!sel.next())
            return 0
        beforeRemove(id, srv)
        val tbl = tableFactory()
        return Delete(tbl)
            .where(eqIdExpr(tbl, id))
            .execute()
    }

    fun tableFactory(): T
    fun generateKey(): Key
    fun setupId(tbl: T, id: Key)
    fun eqIdExpr(tbl: T, id: Key): SqlValue<*, *>
    fun getIdCol(tbl: T): TableColumn<Key, *>
}
