package com.hugin.invoice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

public class HttpServerVerticle extends AbstractVerticle {

    private InvoiceService invoiceService;

    @Override
    public void start(Promise<Void> startPromise) {
        InvoiceRepository manager = new InvoiceManager(vertx);
        this.invoiceService = new InvoiceService(vertx, manager);

        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        // Herhangi bir beklenmeyen hatada düz "Internal Server Error" yerine JSON hata detayı döner
        router.route().failureHandler(ctx -> {
            Throwable failure = ctx.failure();
            String msg = failure != null ? failure.getMessage() : "Bilinmeyen sunucu hatası";
            if (failure != null) failure.printStackTrace();
            ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "application/json; charset=utf-8")
                    .end(new JsonObject().put("error", msg).encode());
        });

        // Müşteri Endpoint'leri
        router.get("/api/customers").handler(ctx -> respond(ctx, invoiceService.getCustomers()));
        router.post("/api/customers").handler(ctx -> respond(ctx, invoiceService.createCustomer(safeBody(ctx))));
        router.put("/api/customers/:id").handler(ctx -> respond(ctx, invoiceService.updateCustomer(pathId(ctx), safeBody(ctx))));
        router.delete("/api/customers/:id").handler(ctx -> respond(ctx, invoiceService.deleteCustomer(pathId(ctx))));

        // Ürün Endpoint'leri
        router.get("/api/items").handler(ctx -> respond(ctx, invoiceService.getItems()));
        router.post("/api/items").handler(ctx -> respond(ctx, invoiceService.createItem(safeBody(ctx))));
        router.put("/api/items/:id").handler(ctx -> respond(ctx, invoiceService.updateItem(pathId(ctx), safeBody(ctx))));
        router.delete("/api/items/:id").handler(ctx -> respond(ctx, invoiceService.deleteItem(pathId(ctx))));

        // Fatura Endpoint'leri
        router.post("/api/invoices/calculate").handler(ctx -> respond(ctx, invoiceService.calculateInvoice(safeBody(ctx))));
        router.get("/api/invoices").handler(ctx -> respond(ctx, invoiceService.getInvoices()));
        router.get("/api/invoices/search").handler(ctx ->
                respond(ctx, invoiceService.getInvoiceBySerialAndNo(ctx.request().getParam("serial"), ctx.request().getParam("no"))));
        router.get("/api/invoices/:id").handler(ctx -> respond(ctx, invoiceService.getInvoiceById(pathId(ctx))));
        router.post("/api/invoices").handler(ctx -> respond(ctx, invoiceService.createInvoice(safeBody(ctx))));
        router.put("/api/invoices/:id").handler(ctx -> respond(ctx, invoiceService.updateInvoice(pathId(ctx), safeBody(ctx))));
        router.delete("/api/invoices/:id").handler(ctx -> respond(ctx, invoiceService.deleteInvoice(pathId(ctx))));

        // XML ve JSON Çıktı Endpoint'leri
        router.post("/api/invoices/:id/export/json").handler(ctx -> respond(ctx, invoiceService.exportJson(pathId(ctx))));
        router.post("/api/invoices/:id/export/xml").handler(ctx -> respond(ctx, invoiceService.exportXml(pathId(ctx))));

        int port = config().getJsonObject("http", new JsonObject()).getInteger("port", 8080);

        vertx.createHttpServer()
                .requestHandler(router)
                .listen(port)
                .onSuccess(server -> {
                    System.out.println("HttpServerVerticle: http://localhost:" + port + " adresinde Postman isteklerini dinliyor!");
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private int pathId(RoutingContext ctx) {
        return Integer.parseInt(ctx.pathParam("id"));
    }

    private JsonObject safeBody(RoutingContext ctx) {
        try {
            Buffer buf = ctx.body().buffer();
            if (buf == null || buf.length() == 0) return new JsonObject();
            String raw = buf.toString("UTF-8").replace('\u00A0', ' ').trim();
            return raw.isEmpty() ? new JsonObject() : new JsonObject(raw);
        } catch (Exception e) {
            return new JsonObject();
        }
    }

    private void respond(RoutingContext ctx, Future<?> future) {
        future.onSuccess(result -> ctx.response()
                        .putHeader("Content-Type", "application/json; charset=utf-8")
                        .end(result.toString()))
                .onFailure(err -> {
                    int statusCode = (err instanceof ReplyException re) ? re.failureCode() : 400;
                    ctx.response()
                            .setStatusCode(statusCode > 0 ? statusCode : 400)
                            .putHeader("Content-Type", "application/json; charset=utf-8")
                            .end(new JsonObject().put("error", err.getMessage()).encode());
                });
    }
}