package com.hugin.invoice;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

public interface InvoiceRepository {

    Future<Object> get(JsonObject query);

    Future<JsonObject> insert(JsonObject data);

    Future<JsonObject> update(JsonObject data);

    Future<JsonObject> delete(JsonObject query);
}