package com.hugin.invoice;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import static com.hugin.invoice.EventBusAddresses.*;

// PostgreSQL Verticle: InvoiceRepository interface'ini implemente eder ve sadece 4 tekil EventBus komutunu dinler
public class PostgresVerticle extends AbstractVerticle implements InvoiceRepository {

    private Pool client;

    @Override
    public void start(Promise<Void> startPromise) {
        JsonObject dbConfig = config().getJsonObject("db", new JsonObject());

        PgConnectOptions connectOptions = new PgConnectOptions()
                .setHost(dbConfig.getString("host", "localhost"))
                .setPort(dbConfig.getInteger("port", 5433))
                .setDatabase(dbConfig.getString("database", "upload_system"))
                .setUser(dbConfig.getString("user", "postgres"))
                .setPassword(dbConfig.getString("password", "1234"));

        PoolOptions poolOptions = new PoolOptions().setMaxSize(dbConfig.getInteger("poolSize", 5));
        client = PgBuilder.pool()
                .with(poolOptions)
                .connectingTo(connectOptions)
                .using(vertx)
                .build();

        String migrationPath = dbConfig.getString("migrationPath", "migration/init.sql");

        vertx.fileSystem().readFile(migrationPath)
                .compose(buffer -> client.query(buffer.toString()).execute())
                .onSuccess(v -> {
                    System.out.println("PostgresVerticle: Migration (" + migrationPath + ") başarıyla çalıştırıldı!");
                    registerConsumers();
                    startPromise.complete();
                })
                .onFailure(err -> {
                    System.err.println("PostgresVerticle Migration hatası: " + err.getMessage());
                    startPromise.fail(err);
                });
    }

    private void registerConsumers() {
        vertx.eventBus().<JsonObject>consumer(DB_GET, msg ->
                get(msg.body()).onSuccess(msg::reply).onFailure(err -> msg.fail(404, err.getMessage())));

        vertx.eventBus().<JsonObject>consumer(DB_INSERT, msg ->
                insert(msg.body()).onSuccess(msg::reply).onFailure(err -> msg.fail(500, err.getMessage())));

        vertx.eventBus().<JsonObject>consumer(DB_UPDATE, msg ->
                update(msg.body()).onSuccess(msg::reply).onFailure(err -> msg.fail(404, err.getMessage())));

        vertx.eventBus().<JsonObject>consumer(DB_DELETE, msg ->
                delete(msg.body()).onSuccess(msg::reply).onFailure(err -> msg.fail(404, err.getMessage())));
    }

    @Override
    public Future<Object> get(JsonObject query) {
        String target = query.getString("target", "invoice");
        return switch (target) {
            case "customer" -> getCustomers().map(r -> r);
            case "item" -> getItems().map(r -> r);
            default -> {
                if (query.containsKey("id")) {
                    yield getInvoiceById(query.getInteger("id")).map(r -> r);
                } else if (query.containsKey("serial") && query.containsKey("no")) {
                    yield getInvoiceBySerialAndNo(query.getString("serial"), query.getString("no")).map(r -> r);
                }
                yield getInvoices().map(r -> r);
            }
        };
    }

    @Override
    public Future<JsonObject> insert(JsonObject data) {
        String target = data.getString("target", "invoice");
        return switch (target) {
            case "customer" -> insertCustomer(data);
            case "item" -> insertItem(data);
            default -> insertInvoice(data);
        };
    }

    @Override
    public Future<JsonObject> update(JsonObject data) {
        String target = data.getString("target", "invoice");
        int id = data.getInteger("id");
        return switch (target) {
            case "customer" -> updateCustomer(id, data);
            case "item" -> updateItem(id, data);
            default -> updateInvoice(id, data);
        };
    }

    @Override
    public Future<JsonObject> delete(JsonObject query) {
        String target = query.getString("target", "invoice");
        int id = query.getInteger("id");
        return switch (target) {
            case "customer" -> deleteCustomer(id);
            case "item" -> deleteItem(id);
            default -> deleteInvoice(id);
        };
    }

    private BigDecimal toDecimal(Double val) {
        return BigDecimal.valueOf(val != null ? val : 0.0);
    }

    // --- POSTGRESQL SORGULARI ($1, $2 ve RETURNING id) ---
    private Future<JsonArray> getCustomers() {
        return client.query("SELECT id, name, tckn FROM customer ORDER BY id DESC").execute().map(rows -> {
            JsonArray list = new JsonArray();
            for (Row row : rows) {
                list.add(new JsonObject().put("id", row.getInteger("id")).put("name", row.getString("name")).put("tckn", row.getString("tckn")));
            }
            return list;
        });
    }

    private Future<JsonObject> insertCustomer(JsonObject body) {
        String name = body.getString("name");
        String tckn = body.getString("tckn");
        return client.preparedQuery("INSERT INTO customer (name, tckn) VALUES ($1, $2) RETURNING id")
                .execute(Tuple.of(name, tckn))
                .map(rows -> new JsonObject().put("id", rows.iterator().next().getInteger("id")).put("name", name).put("tckn", tckn));
    }

    private Future<JsonObject> updateCustomer(int id, JsonObject body) {
        String name = body.getString("name");
        String tckn = body.getString("tckn");
        return client.preparedQuery("UPDATE customer SET name = $1, tckn = $2 WHERE id = $3")
                .execute(Tuple.of(name, tckn, id))
                .compose(res -> res.rowCount() == 0
                        ? Future.failedFuture("Güncellenecek müşteri bulunamadı.")
                        : Future.succeededFuture(new JsonObject().put("id", id).put("name", name).put("tckn", tckn)));
    }

    private Future<JsonObject> deleteCustomer(int id) {
        return client.preparedQuery("DELETE FROM customer WHERE id = $1").execute(Tuple.of(id))
                .compose(res -> res.rowCount() == 0
                        ? Future.failedFuture("Silinecek müşteri bulunamadı.")
                        : Future.succeededFuture(new JsonObject().put("deleted", true).put("id", id)));
    }

    private Future<JsonArray> getItems() {
        return client.query("SELECT id, name, price FROM items ORDER BY id DESC").execute().map(rows -> {
            JsonArray list = new JsonArray();
            for (Row row : rows) {
                list.add(new JsonObject().put("id", row.getInteger("id")).put("name", row.getString("name")).put("price", row.getNumeric("price").doubleValue()));
            }
            return list;
        });
    }

    private Future<JsonObject> insertItem(JsonObject body) {
        String name = body.getString("name");
        Double price = body.getDouble("price");
        return client.preparedQuery("INSERT INTO items (name, price) VALUES ($1, $2) RETURNING id")
                .execute(Tuple.of(name, toDecimal(price)))
                .map(rows -> new JsonObject().put("id", rows.iterator().next().getInteger("id")).put("name", name).put("price", price));
    }

    private Future<JsonObject> updateItem(int id, JsonObject body) {
        String name = body.getString("name");
        Double price = body.getDouble("price");
        return client.preparedQuery("UPDATE items SET name = $1, price = $2 WHERE id = $3")
                .execute(Tuple.of(name, toDecimal(price), id))
                .compose(res -> res.rowCount() == 0
                        ? Future.failedFuture("Güncellenecek ürün bulunamadı.")
                        : Future.succeededFuture(new JsonObject().put("id", id).put("name", name).put("price", price)));
    }

    private Future<JsonObject> deleteItem(int id) {
        return client.preparedQuery("DELETE FROM items WHERE id = $1").execute(Tuple.of(id))
                .compose(res -> res.rowCount() == 0
                        ? Future.failedFuture("Silinecek ürün bulunamadı.")
                        : Future.succeededFuture(new JsonObject().put("deleted", true).put("id", id)));
    }

    private Future<JsonArray> getInvoices() {
        String sql = """
                SELECT i.id, i.serial, i.no, i.total_amount, i.discount_amount, i.payable_amount,
                       c.name AS customer_name, c.tckn AS customer_tckn
                FROM invoices i
                JOIN customer c ON i.customer_id = c.id
                ORDER BY i.id DESC
                """;
        return client.query(sql).execute().map(rows -> {
            JsonArray list = new JsonArray();
            for (Row row : rows) {
                list.add(mapInvoiceRow(row));
            }
            return list;
        });
    }

    private Future<JsonObject> getInvoiceById(int id) {
        String invoiceSql = """
                SELECT i.id, i.serial, i.no, i.total_amount, i.discount_amount, i.payable_amount,
                       c.id AS customer_id, c.name AS customer_name, c.tckn AS customer_tckn
                FROM invoices i
                JOIN customer c ON i.customer_id = c.id
                WHERE i.id = $1
                """;
        return client.preparedQuery(invoiceSql).execute(Tuple.of(id)).compose(rows -> {
            if (!rows.iterator().hasNext()) {
                return Future.failedFuture("Fatura bulunamadı.");
            }
            Row row = rows.iterator().next();
            JsonObject invoice = new JsonObject()
                    .put("id", row.getInteger("id"))
                    .put("serial", row.getString("serial"))
                    .put("no", row.getString("no"))
                    .put("totalAmount", row.getNumeric("total_amount").doubleValue())
                    .put("discountAmount", row.getNumeric("discount_amount").doubleValue())
                    .put("payableAmount", row.getNumeric("payable_amount").doubleValue())
                    .put("customer", new JsonObject()
                            .put("id", row.getInteger("customer_id"))
                            .put("name", row.getString("customer_name"))
                            .put("tckn", row.getString("customer_tckn")));

            String itemsSql = """
                    SELECT ii.quantity, ii.total, it.id AS item_id, it.name, it.price
                    FROM invoice_items ii
                    JOIN items it ON ii.item_id = it.id
                    WHERE ii.invoice_id = $1
                    """;
            return client.preparedQuery(itemsSql).execute(Tuple.of(id)).map(itemRows -> {
                JsonArray itemList = new JsonArray();
                for (Row ir : itemRows) {
                    itemList.add(new JsonObject()
                            .put("itemId", ir.getInteger("item_id"))
                            .put("name", ir.getString("name"))
                            .put("unitPrice", ir.getNumeric("price").doubleValue())
                            .put("quantity", ir.getInteger("quantity"))
                            .put("total", ir.getNumeric("total").doubleValue()));
                }
                return invoice.put("items", itemList);
            });
        });
    }

    private Future<JsonObject> getInvoiceBySerialAndNo(String serial, String no) {
        String sql = """
                SELECT i.id, i.serial, i.no, i.total_amount, i.discount_amount, i.payable_amount,
                       c.name AS customer_name, c.tckn AS customer_tckn
                FROM invoices i
                JOIN customer c ON i.customer_id = c.id
                WHERE i.serial = $1 AND i.no = $2
                """;
        return client.preparedQuery(sql).execute(Tuple.of(serial, no)).compose(rows -> {
            if (!rows.iterator().hasNext()) {
                return Future.failedFuture("Bu Seri ve Numaraya ait fatura bulunamadı.");
            }
            return Future.succeededFuture(mapInvoiceRow(rows.iterator().next()));
        });
    }

    private Future<JsonObject> insertInvoice(JsonObject body) {
        String serial = body.getString("serial");
        String no = body.getString("no");
        Integer customerId = body.getInteger("customerId");
        double totalAmount = body.getDouble("totalAmount");
        double discountAmount = body.getDouble("discountAmount");
        double payableAmount = body.getDouble("payableAmount");
        JsonArray items = body.getJsonArray("items");

        return client.withTransaction(conn -> {
            String insertInvoiceSql = """
                    INSERT INTO invoices (serial, no, customer_id, total_amount, discount_amount, payable_amount)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    RETURNING id
                    """;
            return conn.preparedQuery(insertInvoiceSql)
                    .execute(Tuple.of(serial, no, customerId, toDecimal(totalAmount), toDecimal(discountAmount), toDecimal(payableAmount)))
                    .compose(rows -> {
                        int invoiceId = rows.iterator().next().getInteger("id");
                        String insertItemSql = "INSERT INTO invoice_items (invoice_id, item_id, quantity, total) VALUES ($1, $2, $3, $4)";
                        List<Tuple> batch = new ArrayList<>();
                        for (int i = 0; i < items.size(); i++) {
                            JsonObject item = items.getJsonObject(i);
                            int itemId = item.getInteger("itemId");
                            int quantity = item.getInteger("quantity");
                            double lineTotal = item.getDouble("price") * quantity;
                            batch.add(Tuple.of(invoiceId, itemId, quantity, toDecimal(lineTotal)));
                        }
                        return conn.preparedQuery(insertItemSql).executeBatch(batch).map(res -> new JsonObject()
                                .put("id", invoiceId).put("serial", serial).put("no", no)
                                .put("totalAmount", totalAmount).put("discountAmount", discountAmount).put("payableAmount", payableAmount));
                    });
        });
    }

    private Future<JsonObject> updateInvoice(int id, JsonObject body) {
        String serial = body.getString("serial");
        String no = body.getString("no");
        Integer customerId = body.getInteger("customerId");
        double totalAmount = body.getDouble("totalAmount");
        double discountAmount = body.getDouble("discountAmount");
        double payableAmount = body.getDouble("payableAmount");
        JsonArray items = body.getJsonArray("items");

        return client.withTransaction(conn -> {
            String updateSql = """
                    UPDATE invoices
                    SET serial = $1, no = $2, customer_id = $3,
                        total_amount = $4, discount_amount = $5, payable_amount = $6
                    WHERE id = $7
                    """;
            return conn.preparedQuery(updateSql)
                    .execute(Tuple.of(serial, no, customerId, toDecimal(totalAmount), toDecimal(discountAmount), toDecimal(payableAmount), id))
                    .compose(res -> res.rowCount() == 0
                            ? Future.failedFuture("Güncellenecek fatura bulunamadı.")
                            : conn.preparedQuery("DELETE FROM invoice_items WHERE invoice_id = $1").execute(Tuple.of(id)))
                    .compose(res -> {
                        String insertItemSql = "INSERT INTO invoice_items (invoice_id, item_id, quantity, total) VALUES ($1, $2, $3, $4)";
                        List<Tuple> batch = new ArrayList<>();
                        for (int i = 0; i < items.size(); i++) {
                            JsonObject item = items.getJsonObject(i);
                            int itemId = item.getInteger("itemId");
                            int quantity = item.getInteger("quantity");
                            double lineTotal = item.getDouble("price") * quantity;
                            batch.add(Tuple.of(id, itemId, quantity, toDecimal(lineTotal)));
                        }
                        return conn.preparedQuery(insertItemSql).executeBatch(batch).map(r -> new JsonObject()
                                .put("id", id).put("serial", serial).put("no", no)
                                .put("totalAmount", totalAmount).put("discountAmount", discountAmount).put("payableAmount", payableAmount));
                    });
        });
    }

    private Future<JsonObject> deleteInvoice(int id) {
        return client.withTransaction(conn ->
                conn.preparedQuery("DELETE FROM invoice_items WHERE invoice_id = $1").execute(Tuple.of(id))
                        .compose(res -> conn.preparedQuery("DELETE FROM invoices WHERE id = $1").execute(Tuple.of(id)))
                        .compose(res -> res.rowCount() == 0
                                ? Future.failedFuture("Silinecek fatura bulunamadı.")
                                : Future.succeededFuture(new JsonObject().put("deleted", true).put("id", id)))
        );
    }

    private JsonObject mapInvoiceRow(Row row) {
        return new JsonObject()
                .put("id", row.getInteger("id"))
                .put("serial", row.getString("serial"))
                .put("no", row.getString("no"))
                .put("totalAmount", row.getNumeric("total_amount").doubleValue())
                .put("discountAmount", row.getNumeric("discount_amount").doubleValue())
                .put("payableAmount", row.getNumeric("payable_amount").doubleValue())
                .put("customerName", row.getString("customer_name"))
                .put("customerTckn", row.getString("customer_tckn"));
    }
}