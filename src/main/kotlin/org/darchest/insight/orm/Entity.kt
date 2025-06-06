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

    suspend fun beforeRead(select: ReadableSelect<T>, isCount: Boolean) {

    }

    suspend fun beforeCreate(t: T, id: Key) {

    }

    fun fieldsToCheck(t: T): MutableList<SqlValue<*, *>> = mutableListOf(getIdCol(t))

    suspend fun beforeUpdate(t: T, id: Key, srv: T) {

    }

    suspend fun beforeRemove(id: Key, srv: T) {

    }

    suspend fun read(fs: Array<SqlValue<*, *>>, where: SqlValue<*, *>? = null, sort: Array<SortInfo>? = null, limit: Long? = null, offset: Long? = null, isCount: Boolean = false) {
        val sel = ReadableSelectImp(me)
        sel.fields(*fs)
        sel.where(where)
        if (sort != null)
            sel.sort(*sort)
        sel.limit(limit)
        sel.offset(offset)
        beforeRead(sel, isCount)
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
        return id
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