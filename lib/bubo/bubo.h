#pragma once

#include "node.h"
#include "nan.h"
#include "js-method.h"
#include "bubo-cache.h"

#include <unordered_set>


class Bubo : public node::ObjectWrap {
public:
    static void Init(v8::Handle<v8::Object> exports);
    static NAN_METHOD(New);
    static v8::Persistent<v8::Function> constructor;

private:
    explicit Bubo();
    ~Bubo();

    NAN_METHOD(Initialize);

    JS_METHOD_DECL(LookupPoint);
    JS_METHOD_DECL(RemovePoint);
    JS_METHOD_DECL(RemoveBucket);
    JS_METHOD_DECL(Stats);
    JS_METHOD_DECL(Test);

	BuboCache cache_;
};
