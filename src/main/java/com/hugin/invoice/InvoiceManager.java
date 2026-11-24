package com.hugin.invoice;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import static com.hugin.invoice.EventBusAddresses.*;

public class InvoiceManager implements InvoiceRepository {

    private final Vertx vertx;

    public InvoiceManager(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public Future<Object> get(JsonObject query) {
        return vertx.eventBus().request(DB_GET, query).map(Message::body);
    }

    @Override
    public Future<JsonObject> insert(JsonObject data) {
        return vertx.eventBus().<JsonObject>request(DB_INSERT, data).map(Message::body);
    }

    @Override
    public Future<JsonObject> update(JsonObject data) {
        return vertx.eventBus().<JsonObject>request(DB_UPDATE, data).map(Message::body);
    }

    @Override
    public Future<JsonObject> delete(JsonObject query) {
        return vertx.eventBus().<JsonObject>request(DB_DELETE, query).map(Message::body);
    }
}