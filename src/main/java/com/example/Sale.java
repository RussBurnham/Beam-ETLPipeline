package com.example;

import java.io.Serializable;
import java.time.LocalDate;

public class Sale implements Serializable {
    private String productId;
    private String customerId;
    private int quantity;
    private LocalDate saleDate;

    public Sale() {
    }

    public Sale(String productId, String customerId, int quantity, LocalDate saleDate) {
        this.productId = productId;
        this.customerId = customerId;
        this.quantity = quantity;
        this.saleDate = saleDate;
    }

    // Getters and setters
    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public LocalDate getSaleDate() {
        return saleDate;
    }

    public void setSaleDate(LocalDate saleDate) {
        this.saleDate = saleDate;
    }
}
