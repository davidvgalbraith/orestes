#include <stdlib.h>

#include "bubo.h"
#include "utils.h"
#include "persistent-string.h"
#include "test.h"


using namespace v8;

Persistent<Function> Bubo::constructor;

NAN_METHOD(NewInstance) {
    NanScope();

    const unsigned argc = 1;
    Local<Value> argv[argc] = {args[0]};
    Local<Function> cons = NanNew<Function>(Bubo::constructor);
    Local<Object> instance = cons->NewInstance(argc, argv);

    NanReturnValue(instance);
}

NAN_METHOD(Bubo::New)
{
    NanScope();

    if (!args.IsConstructCall()) {
        return NanThrowError("non-constructor invocation not supported");
    }

    Bubo* obj = new Bubo();
    obj->Wrap(args.This());

    obj->Initialize(args);

    NanReturnValue(args.This());
}

Bubo::Bubo()
{
    printf("bubo is alive!\n");
}

Bubo::~Bubo()
{
    printf("bubo is dead :(\n");
}

NAN_METHOD(Bubo::Initialize)
{
    if (args[0]->IsUndefined()) {
        return NanThrowError("must initialize Bubo with ignored attributes");
    }

    Local<Object> opts = args[0].As<Object>();

    if (opts->Get(NanNew("ignoredAttributes"))->IsUndefined()) {
        return NanThrowError("must initialize Bubo with ignored attributes");
    }

    bubo_utils::initialize(opts);
    NanReturnUndefined();
}

JS_METHOD(Bubo, LookupPoint)
{
    NanScope();

    if (args.Length() < 3) {
        return NanThrowError("LookupPoint: invalid arguments");
    }

    Local<String> spaceBucket = args[0].As<String>();
    Local<Object> obj = args[1].As<Object>();
    Local<Object> result = args[2].As<Object>();

    Local<String> attrs;
    int error_value = 0;
    int* error = &error_value;
    bool found = cache_.lookup(spaceBucket, obj, attrs, error);

    if (*error) {
        return NanThrowError("point too big");
    }

    static PersistentString founded("found");
    static PersistentString attr_str("attr_str");

    result->Set(founded, NanNew<Boolean>(found));
    result->Set(attr_str, attrs);

    NanReturnUndefined();
}


JS_METHOD(Bubo, RemovePoint)
{
    NanScope();

    if (args.Length() < 2) {
        return NanThrowError("RemovePoint: invalid arguments");
    }

    Local<String> spaceBucket = args[0].As<String>();
    Local<Object> obj = args[1].As<Object>();

    cache_.remove(spaceBucket, obj);

    NanReturnUndefined();
}


JS_METHOD(Bubo, RemoveBucket)
{
    NanScope();

    if (args.Length() < 1) {
        return NanThrowError("RemoveBucket: invalid arguments");
    }

    Local<String> spaceBucket = args[0].As<String>();
    cache_.remove_bucket(spaceBucket);

    NanReturnUndefined();
}

JS_METHOD(Bubo, Test)
{
    NanScope();

    testall();

    NanReturnUndefined();
}

JS_METHOD(Bubo, Stats)
{
    NanScope();

    if (args.Length() < 1) {
        return NanThrowError("Stats: invalid arguments");
    }

    Local<Object> stats = args[0].As<Object>();
    cache_.stats(stats);

    NanReturnUndefined();
}

void
Bubo::Init(Handle<Object> exports)
{
    NanScope();

    // Prepare constructor template
    Local<FunctionTemplate> tpl = NanNew<FunctionTemplate>(Bubo::New);
    tpl->SetClassName(NanNew("Bubo"));
    tpl->InstanceTemplate()->SetInternalFieldCount(1);

    // Prototype
    NODE_SET_PROTOTYPE_METHOD(tpl, "lookup_point", JS_METHOD_NAME(LookupPoint));
    NODE_SET_PROTOTYPE_METHOD(tpl, "remove_point", JS_METHOD_NAME(RemovePoint));
    NODE_SET_PROTOTYPE_METHOD(tpl, "remove_bucket", JS_METHOD_NAME(RemoveBucket));
    NODE_SET_PROTOTYPE_METHOD(tpl, "test", JS_METHOD_NAME(Test));
    NODE_SET_PROTOTYPE_METHOD(tpl, "stats", JS_METHOD_NAME(Stats));
    NanAssignPersistent(constructor, tpl->GetFunction());

    exports->Set(NanNew("Bubo"),
        NanNew<FunctionTemplate>(NewInstance)->GetFunction());
}

void
InitModule(Handle<Object> exports)
{
    Bubo::Init(exports);
}

NODE_MODULE(bubo, InitModule)
