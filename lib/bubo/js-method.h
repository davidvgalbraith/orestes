// Simple macros to simplify the boilerplate of declaring JS-exposed methods
// on an object.

#pragma once

#define JS_METHOD_NAME(_name) JS##_name

#define JS_METHOD_DECL(_name) \
    static NAN_METHOD(JS_METHOD_NAME(_name)); \
    NAN_METHOD(_name);

#define JS_METHOD(_cls, _name) \
    NAN_METHOD(_cls::JS_METHOD_NAME(_name)) { \
        _cls* obj = ObjectWrap::Unwrap<_cls>(args.Holder()); \
        return obj->_name(args); \
    } \
    NAN_METHOD(_cls::_name)
