/*
 * Copyright 2021-2026, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.orm.ksp

import com.google.devtools.ksp.getDeclaredFunctions
import com.google.devtools.ksp.getDeclaredProperties
import com.google.devtools.ksp.isPublic
import com.google.devtools.ksp.processing.CodeGenerator
import com.google.devtools.ksp.processing.Dependencies
import com.google.devtools.ksp.processing.KSPLogger
import com.google.devtools.ksp.processing.Resolver
import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.symbol.ClassKind
import com.google.devtools.ksp.symbol.KSAnnotated
import com.google.devtools.ksp.symbol.KSAnnotation
import com.google.devtools.ksp.symbol.KSClassDeclaration
import com.google.devtools.ksp.symbol.KSPropertyDeclaration
import com.google.devtools.ksp.symbol.KSType
import org.darchest.insight.orm.annotations.AutoEntity
import org.darchest.insight.orm.annotations.DefaultEntityParent
import org.darchest.insight.orm.annotations.UnspecifiedEntityParent
import java.io.OutputStreamWriter

class AutoEntityProcessor(
    private val codeGenerator: CodeGenerator,
    private val logger: KSPLogger,
    private val options: Map<String, String>
) : SymbolProcessor {

    override fun process(resolver: Resolver): List<KSAnnotated> {
        val tables = resolver.getSymbolsWithAnnotation(AutoEntity::class.qualifiedName!!)
            .filterIsInstance<KSClassDeclaration>()
            .toList()

        if (tables.isEmpty()) return emptyList()

        val template = options[CLASS_NAME_TEMPLATE_OPTION] ?: "*"
        if (template.count { it == '*' } != 1) {
            logger.error(
                "KSP option '$CLASS_NAME_TEMPLATE_OPTION' must contain exactly one '*', got: \"$template\""
            )
            return emptyList()
        }

        val defaultParents = resolver.getSymbolsWithAnnotation(DefaultEntityParent::class.qualifiedName!!)
            .filterIsInstance<KSClassDeclaration>()
            .toList()

        for (table in tables) {
            processTable(table, template, defaultParents)
        }

        return emptyList()
    }

    private fun processTable(
        table: KSClassDeclaration,
        template: String,
        defaultParents: List<KSClassDeclaration>
    ) {
        val tableName = table.simpleName.asString()
        if (!tableName.endsWith("Table")) {
            logger.error("Class annotated with @AutoEntity must end with 'Table': $tableName", table)
            return
        }

        val stem = tableName.removeSuffix("Table")
        val entityName = template.replace("*", stem)

        val base = resolveBase(table, defaultParents) ?: return
        val baseName = base.simpleName.asString()
        val baseQualified = base.qualifiedName?.asString()
        if (baseQualified == null) {
            logger.error("Entity base class must have a qualified name", base)
            return
        }

        val tableQualified = table.qualifiedName?.asString()
        if (tableQualified == null) {
            logger.error("Table class must have a qualified name", table)
            return
        }

        val tablePackage = table.packageName.asString()
        if (tablePackage.isEmpty() || !tablePackage.contains('.')) {
            logger.error("Table class must be in a nested package to derive autoentity package", table)
            return
        }
        val genPackage = tablePackage.substringBeforeLast('.') + ".autoentity"

        val skipNames = collectMemberNames(base)
        val properties = collectTableProperties(table)
            .filter { it.simpleName.asString() !in skipNames }

        val containingFile = table.containingFile
        if (containingFile == null) {
            logger.error("Cannot find containing file for $tableName", table)
            return
        }

        val originFiles = buildList {
            add(containingFile)
            defaultParents.mapNotNullTo(this) { it.containingFile }
            base.containingFile?.let { add(it) }
        }.distinct()

        val file = codeGenerator.createNewFile(
            Dependencies(aggregating = true, *originFiles.toTypedArray()),
            genPackage,
            entityName
        )

        OutputStreamWriter(file, Charsets.UTF_8).use { writer ->
            writer.write("package $genPackage\n\n")
            writer.write("import $baseQualified\n")
            writer.write("import $tableQualified\n\n")
            writer.write("open class $entityName : $baseName<$tableName>(::$tableName) {\n")
            for (prop in properties) {
                val name = prop.simpleName.asString()
                writer.write("    val $name get() = me.$name\n")
            }
            writer.write("}\n")
        }
    }

    private fun resolveBase(
        table: KSClassDeclaration,
        defaultParents: List<KSClassDeclaration>
    ): KSClassDeclaration? {
        val annotation = table.annotations.firstOrNull {
            it.shortName.asString() == "AutoEntity" &&
                it.annotationType.resolve().declaration.qualifiedName?.asString() == AutoEntity::class.qualifiedName
        } ?: run {
            logger.error("Missing @AutoEntity annotation", table)
            return null
        }

        val baseType = annotation.argumentAsType("base")
        val baseDeclaration = baseType?.declaration as? KSClassDeclaration
        val baseQualified = baseDeclaration?.qualifiedName?.asString()

        if (baseQualified != null && baseQualified != UnspecifiedEntityParent::class.qualifiedName) {
            return baseDeclaration
        }

        return when (defaultParents.size) {
            0 -> {
                logger.error(
                    "No entity base specified on @AutoEntity and no @DefaultEntityParent found in the project",
                    table
                )
                null
            }
            1 -> defaultParents[0]
            else -> {
                logger.error(
                    "Multiple @DefaultEntityParent classes found: " +
                        defaultParents.joinToString { it.qualifiedName?.asString() ?: it.simpleName.asString() },
                    table
                )
                null
            }
        }
    }

    private fun collectTableProperties(table: KSClassDeclaration): List<KSPropertyDeclaration> {
        val layers = mutableListOf<List<KSPropertyDeclaration>>()
        var current: KSClassDeclaration? = table
        while (current != null) {
            val qualified = current.qualifiedName?.asString()
            if (qualified == null || qualified in STOP_SUPERTYPES) {
                break
            }
            val props = current.getDeclaredProperties()
                .filter { it.isPublic() && !it.isMutable }
                .toList()
            layers += props
            current = current.superTypes
                .mapNotNull { it.resolve().declaration as? KSClassDeclaration }
                .firstOrNull { it.classKind == ClassKind.CLASS }
        }
        return layers.asReversed().flatten()
    }

    private fun collectMemberNames(base: KSClassDeclaration): Set<String> {
        val names = mutableSetOf<String>()
        var current: KSClassDeclaration? = base
        while (current != null) {
            val qualified = current.qualifiedName?.asString()
            if (qualified == null || qualified == "kotlin.Any") {
                break
            }
            current.getDeclaredProperties().forEach { names += it.simpleName.asString() }
            current.getDeclaredFunctions().forEach { names += it.simpleName.asString() }
            current = current.superTypes
                .mapNotNull { it.resolve().declaration as? KSClassDeclaration }
                .firstOrNull {
                    it.classKind == ClassKind.CLASS || it.classKind == ClassKind.INTERFACE
                }
        }
        return names
    }

    private fun KSAnnotation.argumentAsType(name: String): KSType? {
        val value = arguments.firstOrNull { it.name?.asString() == name }?.value
        return value as? KSType
    }

    companion object {
        const val CLASS_NAME_TEMPLATE_OPTION = "org.darchest.insight.orm.classNameTemplate"

        private val STOP_SUPERTYPES = setOf(
            "org.darchest.insight.Table",
            "org.darchest.insight.SqlDataSource",
            "kotlin.Any"
        )
    }
}
