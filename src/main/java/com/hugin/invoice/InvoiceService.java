package com.hugin.invoice;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;

public class InvoiceService {

    private final Vertx vertx;
    private final InvoiceRepository manager;

    public InvoiceService(Vertx vertx, InvoiceRepository manager) {
        this.vertx = vertx;
        this.manager = manager;
    }

    // Hem 2000, hem 2000.50, hem de "2000,50" gibi küsuratlı sayıları güvenle okur
    private Double parseDecimal(JsonObject obj, String key) {
        Object val = obj.getValue(key);
        if (val == null) return null;
        if (val instanceof Number num) return num.doubleValue();
        try {
            return Double.parseDouble(val.toString().trim().replace(",", "."));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    // --- MÜŞTERİ İŞLEMLERİ ---
    public Future<Object> getCustomers() {
        return manager.get(new JsonObject().put("target", "customer"));
    }

    public Future<JsonObject> createCustomer(JsonObject body) {
        String name = body.getString("name");
        String tckn = body.getString("tckn");
        if (name == null || name.isBlank() || tckn == null || tckn.length() != 11) {
            return Future.failedFuture("Geçerli bir Ad-Soyad ve 11 haneli TCKN giriniz.");
        }
        return manager.insert(body.copy().put("target", "customer"));
    }

    public Future<JsonObject> updateCustomer(int id, JsonObject body) {
        String name = body.getString("name");
        String tckn = body.getString("tckn");
        if (name == null || name.isBlank() || tckn == null || tckn.length() != 11) {
            return Future.failedFuture("Ad-Soyad ve 11 haneli TCKN zorunludur.");
        }
        return manager.update(body.copy().put("target", "customer").put("id", id));
    }

    public Future<JsonObject> deleteCustomer(int id) {
        return manager.delete(new JsonObject().put("target", "customer").put("id", id));
    }

    // --- ÜRÜN İŞLEMLERİ ---
    public Future<Object> getItems() {
        return manager.get(new JsonObject().put("target", "item"));
    }

    public Future<JsonObject> createItem(JsonObject body) {
        String name = body.getString("name");
        Double price = parseDecimal(body, "price");
        if (name == null || name.isBlank() || price == null || price <= 0) {
            return Future.failedFuture("Geçerli bir ürün adı ve 0'dan büyük fiyat giriniz.");
        }
        return manager.insert(new JsonObject()
                .put("target", "item")
                .put("name", name.trim())
                .put("price", price));
    }

    public Future<JsonObject> updateItem(int id, JsonObject body) {
        String name = body.getString("name");
        Double price = parseDecimal(body, "price");
        if (name == null || name.isBlank() || price == null || price <= 0) {
            return Future.failedFuture("Ürün adı ve 0'dan büyük fiyat zorunludur.");
        }
        return manager.update(new JsonObject()
                .put("target", "item")
                .put("id", id)
                .put("name", name.trim())
                .put("price", price));
    }

    public Future<JsonObject> deleteItem(int id) {
        return manager.delete(new JsonObject().put("target", "item").put("id", id));
    }

    // --- FATURA İŞLEMLERİ ---
    public Future<JsonObject> calculateInvoice(JsonObject body) {
        try {
            return Future.succeededFuture(computeSummary(body));
        } catch (IllegalArgumentException e) {
            return Future.failedFuture(e.getMessage());
        }
    }

    public Future<Object> getInvoices() {
        return manager.get(new JsonObject().put("target", "invoice"));
    }

    public Future<Object> getInvoiceById(int id) {
        return manager.get(new JsonObject().put("target", "invoice").put("id", id));
    }

    public Future<Object> getInvoiceBySerialAndNo(String serial, String no) {
        return manager.get(new JsonObject().put("target", "invoice").put("serial", serial).put("no", no));
    }

    public Future<JsonObject> createInvoice(JsonObject body) {
        String serial = body.getString("serial");
        String no = body.getString("no");
        Integer customerId = body.getInteger("customerId");
        JsonArray items = body.getJsonArray("items", new JsonArray());

        if (serial == null || serial.isBlank() || no == null || no.isBlank() || customerId == null || items.isEmpty()) {
            return Future.failedFuture("Seri, No, Müşteri ve en az 1 ürün zorunludur.");
        }

        try {
            JsonObject payload = body.copy().mergeIn(computeSummary(body)).put("target", "invoice");
            return manager.insert(payload);
        } catch (IllegalArgumentException e) {
            return Future.failedFuture(e.getMessage());
        }
    }

    public Future<JsonObject> updateInvoice(int id, JsonObject body) {
        String serial = body.getString("serial");
        String no = body.getString("no");
        Integer customerId = body.getInteger("customerId");
        JsonArray items = body.getJsonArray("items", new JsonArray());

        if (serial == null || serial.isBlank() || no == null || no.isBlank() || customerId == null || items.isEmpty()) {
            return Future.failedFuture("Seri, No, Müşteri ve en az 1 ürün zorunludur.");
        }

        try {
            JsonObject payload = body.copy().mergeIn(computeSummary(body)).put("target", "invoice").put("id", id);
            return manager.update(payload);
        } catch (IllegalArgumentException e) {
            return Future.failedFuture(e.getMessage());
        }
    }

    public Future<JsonObject> deleteInvoice(int id) {
        return manager.delete(new JsonObject().put("target", "invoice").put("id", id));
    }

    // --- XML VE JSON SERVİSİ ÇIKTI İŞLEMLERİ ---
    public Future<JsonObject> exportJson(int id) {
        return getInvoiceById(id).compose(res -> {
            JsonObject invoice = (JsonObject) res;
            String fileName = "invoice_" + invoice.getString("serial") + "_" + invoice.getString("no") + ".json";
            return vertx.fileSystem().writeFile(fileName, Buffer.buffer(invoice.encodePrettily(), StandardCharsets.UTF_8.name()))
                    .map(v -> new JsonObject()
                            .put("status", "success")
                            .put("format", "JSON")
                            .put("fileName", fileName)
                            .put("invoice", invoice));
        });
    }

    public Future<JsonObject> exportXml(int id) {
        return getInvoiceById(id).compose(res -> {
            JsonObject invoice = (JsonObject) res;
            String fileName = "invoice_" + invoice.getString("serial") + "_" + invoice.getString("no") + ".xml";
            String xmlContent = buildInvoiceXml(invoice);
            return vertx.fileSystem().writeFile(fileName, Buffer.buffer(xmlContent, StandardCharsets.UTF_8.name()))
                    .map(v -> new JsonObject()
                            .put("status", "success")
                            .put("format", "XML")
                            .put("fileName", fileName)
                            .put("xmlContent", xmlContent));
        });
    }

    private JsonObject computeSummary(JsonObject body) {
        JsonArray items = body.getJsonArray("items", new JsonArray());
        String discountType = body.getString("discountType", "AMOUNT");
        Double discountValObj = parseDecimal(body, "discountValue");
        double discountValue = discountValObj != null ? discountValObj : 0.0;

        if (items.isEmpty()) {
            throw new IllegalArgumentException("Sepette en az 1 ürün olmalı.");
        }

        double totalAmount = 0.0;
        for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.getJsonObject(i);
            Double price = parseDecimal(item, "price");
            if (price == null || price <= 0) {
                throw new IllegalArgumentException("Ürün fiyatı geçersiz.");
            }
            item.put("price", price);
            int quantity = item.getInteger("quantity", 0);
            totalAmount += (price * quantity);
        }

        double discountAmount;
        if ("PERCENT".equalsIgnoreCase(discountType)) {
            if (discountValue < 0.0 || discountValue > 100.0) {
                throw new IllegalArgumentException("İndirim oranı %0 ile %100 arasında olmalı!");
            }
            discountAmount = totalAmount * (discountValue / 100.0);
        } else {
            if (discountValue < 0.0) {
                throw new IllegalArgumentException("İndirim tutarı negatif olamaz!");
            }
            if (discountValue > totalAmount) {
                throw new IllegalArgumentException("İndirim tutarı (" + discountValue + "), toplam tutardan (" + totalAmount + ") büyük olamaz!");
            }
            discountAmount = discountValue;
        }

        return new JsonObject()
                .put("totalAmount", totalAmount)
                .put("discountAmount", discountAmount)
                .put("payableAmount", totalAmount - discountAmount);
    }

    private String buildInvoiceXml(JsonObject inv) {
        JsonObject customer = inv.getJsonObject("customer", new JsonObject());
        JsonArray items = inv.getJsonArray("items", new JsonArray());

        StringBuilder sb = new StringBuilder();
        sb.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Invoice>\n");
        sb.append("    <Id>").append(inv.getInteger("id")).append("</Id>\n");
        sb.append("    <Serial>").append(inv.getString("serial")).append("</Serial>\n");
        sb.append("    <No>").append(inv.getString("no")).append("</No>\n");
        sb.append("    <Customer>\n");
        sb.append("        <Name>").append(customer.getString("name")).append("</Name>\n");
        sb.append("        <TCKN>").append(customer.getString("tckn")).append("</TCKN>\n");
        sb.append("    </Customer>\n    <Items>\n");
        for (int i = 0; i < items.size(); i++) {
            JsonObject item = items.getJsonObject(i);
            sb.append("        <Item>\n");
            sb.append("            <Name>").append(item.getString("name")).append("</Name>\n");
            sb.append("            <UnitPrice>").append(item.getDouble("unitPrice")).append("</UnitPrice>\n");
            sb.append("            <Quantity>").append(item.getInteger("quantity")).append("</Quantity>\n");
            sb.append("            <Total>").append(item.getDouble("total")).append("</Total>\n");
            sb.append("        </Item>\n");
        }
        sb.append("    </Items>\n    <Summary>\n");
        sb.append("        <TotalAmount>").append(inv.getDouble("totalAmount")).append("</TotalAmount>\n");
        sb.append("        <DiscountAmount>").append(inv.getDouble("discountAmount")).append("</DiscountAmount>\n");
        sb.append("        <PayableAmount>").append(inv.getDouble("payableAmount")).append("</PayableAmount>\n");
        sb.append("    </Summary>\n</Invoice>\n");
        return sb.toString();
    }
}