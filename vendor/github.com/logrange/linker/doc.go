// Copyright 2018 The logrange Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package linker provides Dependency Injection and Inversion of Control functionality.
The core component is Injector, which allows to register Components. Component
is an object, which can have any type, which requires some initialization, or can be used
for initializing other components. Every component is registered in the Injector
by the component name or anonymously (empty name). Same object can be registered
by different names. This could be useful if the object implements different
interfaces that can be used by different components.

The package contains several interfaces: PostConstructor, Initializer and
Shutdowner, which could be implemented by components with a purpose to be called
by Injector on different initialization/de-initialization phases.

Init() function of Injector allows to initialize registered components. The
initialization process supposes that components with 'pointer to struct' type
or interfaces, which contains a 'pointer to struct' will be initialized. The
initialization supposes to inject (assign) the struct fields values using other
registered components. Injector matches them by name or by type. Injector uses
fail-fast strategy so any error is considered like misconfiguraion and a panic
happens.

When all components are initialized, the components, which implement PostConstructor
interface will be notified via PostConsturct() function call. The order of
PostConstruct() calls is not defined.

After the construction phase, injector builds dependencies graph with a purpose
to detect dependency loops and to establish components initialization order.
If a dependency loop is found, Injector will panic. Components, which implement
Initializer interface, will be notified in specific order by Init(ctx) function
call. Less dependant components will be initialized before the components that
have dependency on the first ones.

Injector is supposed to be called from one go-routine and doesn't support calls
from multiple go-routines.

Initialization process could take significant time, so context is provided. If
the context is cancelled or closed it will be detected either by appropriate
component or by the Injector what will cause of de-intializing already initialized
components using Shutdown() function call (if provided) in reverse of the
initialization order. Panic will happen then.

*/

package linker
