# Linker

[![Go Report Card](https://goreportcard.com/badge/logrange/linker)](https://goreportcard.com/report/logrange/linker) [![Build Status](https://travis-ci.com/logrange/linker.svg?branch=master)](https://travis-ci.com/logrange/linker) [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/logrange/linker/blob/master/LICENSE) [![GoDoc](https://godoc.org/github.com/logrange/linker?status.png)](https://godoc.org/github.com/logrange/linker)

Linker is Dependency Injection and Inversion of Control package. 

**Linker's highlights:**
 - Components registry
 - Dependency injection
 - Components initialization prioritization
 - Initialization and shutdown control
 
```golang

import (
     "github.com/lograng/linker"
)

type DatabaseAccessService interface {
    RunQuery(query string) DbResult
}

// MySQLAccessService implements DatabaseAccessService
type MySQLAccessService struct {
	// Conns uses field's tag to specify injection param name(mySqlConns)
	// or sets-up the default value(32), if the param is not provided 
    Conns int `inject:"mySqlConns, optional:32"`
}

type BigDataService struct {
	// DBa has DatabaseAccessService type which value will be injected by the injector
	// in its Init() function, or it fails if there is no appropriate component with the name(dba)
	// was registered...
    DBa DatabaseAccessService `inject:"dba"`
}
...

func main() {
    // 1st step is to create the injector
    inj := linker.New()
	
    // 2nd step is to register components
    inj.Register(
		linker.Component{Name: "dba", Value: &MySQLAccessService{}},
		linker.Component{Name: "", Value: &BigDataService{}},
		linker.Component{Name: "mySqlConns", Value: int(msconns)},
		...
	)
	
	// 3rd step is to inject dependecies and initialize the registered components
	inj.Init(ctx)
	
	// the injector fails-fast, so if no panic everything is good so far.
	
	...
	// 4th de-initialize all compoments properly
	inj.Shutdown()
}

```
### Annotate fields using fields tags
The `inject` tag field has the following format:
```
inject: "<name>[,optional[:<defaultValue]]"
```
So annotated fields can be assigned using different rules:
```golang
// Field will be assigned by component with registration name "compName",
// if there is no comonent with the name, or it could not be assigned to the type 
// FieldType, panic will happen
Field FieldType `inject:"compName"`

// Field will be assigned by component with any name (indicated as ""), which could be 
// assigned to the FieldType. If no such component or many matches to the type, 
// panic will happen.
Field FieldType `inject:""`

// If no components match to either Field1 or Field2 they will be skipped with 
// no panic. Ambigious situation still panics
Field1 FieldType `inject:"aaa, optional"`
Field2 FieldType `inject:", optional"`

// Default values could be provided for numeric and string types. The 
// default values will be assigned if no components match to the rules
NumFld int `inject:"intFld, optional: 21"`
StrFld string `inject:"strFld,optional:abc"`
```
### Create the injector
Injector is a main object, which  controls the components: registers them, initializes, checks and provides initialization and shutdown calls.
```golang
inj := linker.New()
```
### Register components using names or anonymously
Component is an object that can be used for initialization of other components, or which requires an initialization. Components can have different types, but only fields of components, with 'pointer to struct' type, could be assigned by the Injector. Injector is responsible for the injection(initialization a component's fields) process. All components must be registered in injector via `Register()` function before the initialization process will be run.
### Initialize components
When all components are registered `Init()` function of the `Injector` allows to perform initialization. The `Init()` function does the following actions:
1. Walks over all registered components and assigns all tagged fields using named and unnamed components. If no matches or ambiguity happens, the `Init()` can panic.
2. For components, which implement [linker.PostConstructor](https://github.com/logrange/linker/blob/5dbea0d87b81a70e721f6c359d5f28dc1a01e080/inject.go#L74) interface, the `PostConstruct()` function will be called.
3. For components, which implements [linker.Initializer](https://github.com/logrange/linker/blob/5dbea0d87b81a70e721f6c359d5f28dc1a01e080/inject.go#L90) interface, the `Init(ctx)` function will be called in a specific order. The initialization order is defined as following: less dependent components are initialized before the components 
4. If circular dependency between registered components is found, Init() will panic.
### Shutting down registered components properly
Properly initialized components could be shut-down in back-initialization order by calling `Shutdown()` function of the injector. Components, that implement [linker.Shutdowner](https://github.com/logrange/linker/blob/5dbea0d87b81a70e721f6c359d5f28dc1a01e080/inject.go#L108) interface, will be called by the `Shutdown()`
