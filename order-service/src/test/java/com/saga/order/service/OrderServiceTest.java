package com.saga.order.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.saga.order.entity.Order;
import com.saga.order.entity.enums.OrderStatus;
import com.saga.order.repository.OrderRepository;
import com.saga.order.repository.OutboxEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    private OrderService orderService;

    @BeforeEach
    void setUp() {
        orderService = new OrderService(orderRepository, outboxEventRepository,
                new ObjectMapper().registerModule(new JavaTimeModule()));
    }

    @Test
    void createOrder_withNewIdempotencyKey_createsOrderAndSetsKey() {
        String idempotencyKey = "saga-1:ORDER_CREATE";
        when(orderRepository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            order.setId(UUID.randomUUID());
            return order;
        });

        Order result = orderService.createOrder("user-1", "product-1", 2, BigDecimal.TEN, idempotencyKey);

        assertThat(result.getIdempotencyKey()).isEqualTo(idempotencyKey);
        assertThat(result.getStatus()).isEqualTo(OrderStatus.PENDING);
        assertThat(result.getUnitPrice()).isEqualByComparingTo("10.00");
        assertThat(result.getTotalAmount()).isEqualByComparingTo("20.00");
        verify(orderRepository).save(any(Order.class));
        verify(outboxEventRepository).save(any());
    }

    @Test
    void createOrder_withNullIdempotencyKey_alwaysCreatesNewOrder() {
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> {
            Order order = invocation.getArgument(0);
            order.setId(UUID.randomUUID());
            return order;
        });

        Order result = orderService.createOrder("user-1", "product-1", 2, BigDecimal.TEN, null);

        assertThat(result.getIdempotencyKey()).isNull();
        verify(orderRepository, never()).findByIdempotencyKey(any());
        verify(orderRepository).save(any(Order.class));
    }

    @Test
    void createOrder_withExistingIdempotencyKey_returnsExistingOrderWithoutSaving() {
        String idempotencyKey = "saga-1:ORDER_CREATE";
        Order existing = Order.builder()
                .id(UUID.randomUUID())
                .userId("user-1")
                .productId("product-1")
                .quantity(2)
                .unitPrice(BigDecimal.TEN)
                .totalAmount(BigDecimal.valueOf(20))
                .status(OrderStatus.PENDING)
                .idempotencyKey(idempotencyKey)
                .build();
        when(orderRepository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.of(existing));

        Order result = orderService.createOrder("user-1", "product-1", 2, BigDecimal.TEN, idempotencyKey);

        assertThat(result).isEqualTo(existing);
        verify(orderRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void createOrder_onRaceConditionDuringSave_returnsExistingOrder() {
        String idempotencyKey = "saga-1:ORDER_CREATE";
        Order raced = Order.builder()
                .id(UUID.randomUUID())
                .userId("user-1")
                .productId("product-1")
                .quantity(2)
                .unitPrice(BigDecimal.TEN)
                .totalAmount(BigDecimal.valueOf(20))
                .status(OrderStatus.PENDING)
                .idempotencyKey(idempotencyKey)
                .build();
        when(orderRepository.findByIdempotencyKey(idempotencyKey))
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(raced));
        when(orderRepository.save(any(Order.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate key"));

        Order result = orderService.createOrder("user-1", "product-1", 2, BigDecimal.TEN, idempotencyKey);

        assertThat(result).isEqualTo(raced);
        verify(outboxEventRepository, never()).save(any());
    }

    @Test
    void createOrder_onRaceConditionAndStillNotFound_rethrowsException() {
        String idempotencyKey = "saga-1:ORDER_CREATE";
        when(orderRepository.findByIdempotencyKey(idempotencyKey)).thenReturn(Optional.empty());
        when(orderRepository.save(any(Order.class)))
                .thenThrow(new DataIntegrityViolationException("duplicate key"));

        assertThatThrownBy(() ->
                orderService.createOrder("user-1", "product-1", 2, BigDecimal.TEN, idempotencyKey))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
