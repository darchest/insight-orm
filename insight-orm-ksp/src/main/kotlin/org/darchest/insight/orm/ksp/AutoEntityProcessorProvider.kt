/*
 * Copyright 2021-2026, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.orm.ksp

import com.google.devtools.ksp.processing.SymbolProcessor
import com.google.devtools.ksp.processing.SymbolProcessorEnvironment
import com.google.devtools.ksp.processing.SymbolProcessorProvider

class AutoEntityProcessorProvider : SymbolProcessorProvider {
    override fun create(environment: SymbolProcessorEnvironment): SymbolProcessor {
        return AutoEntityProcessor(environment.codeGenerator, environment.logger, environment.options)
    }
}
