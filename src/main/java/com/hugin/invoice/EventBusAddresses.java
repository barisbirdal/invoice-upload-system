package com.hugin.invoice;

public final class EventBusAddresses {

    private EventBusAddresses() {}

    // Sadece 4 tekil EventBus komutu
    public static final String DB_GET = "db.get";
    public static final String DB_INSERT = "db.insert";
    public static final String DB_UPDATE = "db.update";
    public static final String DB_DELETE = "db.delete";
}