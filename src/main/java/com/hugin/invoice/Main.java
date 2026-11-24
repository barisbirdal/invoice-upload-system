package com.hugin.invoice;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class Main {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();

        vertx.fileSystem().readFile("config.json")
                .compose(buffer -> {
                    JsonObject config = buffer.toJsonObject();
                    DeploymentOptions options = new DeploymentOptions().setConfig(config);

                    return vertx.deployVerticle(new MsSqlVerticle(), options)
                            .compose(id -> vertx.deployVerticle(new HttpServerVerticle(), options));
                })
                .onSuccess(id -> System.out.println("Tüm sistem config.json ayarlarıyla başarıyla başlatıldı!"))
                .onFailure(err -> System.err.println("Sistem başlatılamadı: " + err.getMessage()));
    }
}