package dev.demo.payflow.processor;

import java.math.BigDecimal;

record Account(String accountId, String currency, BigDecimal balance) {
}
