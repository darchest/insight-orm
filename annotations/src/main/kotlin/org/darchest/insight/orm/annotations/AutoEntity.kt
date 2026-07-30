/*
 * Copyright 2021-2026, Darchest and contributors.
 * Licensed under the Apache License, Version 2.0
 */

package org.darchest.insight.orm.annotations

import kotlin.reflect.KClass

@Target(AnnotationTarget.CLASS)
annotation class AutoEntity(
    val base: KClass<*> = UnspecifiedEntityParent::class
)
