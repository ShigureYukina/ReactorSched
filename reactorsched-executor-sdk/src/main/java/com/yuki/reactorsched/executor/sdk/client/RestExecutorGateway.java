package com.yuki.reactorsched.executor.sdk.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.executor.sdk.ExecutorSdkProperties;
import com.yuki.reactorsched.executor.sdk.dto.ClaimTasksRequest;
import com.yuki.reactorsched.executor.sdk.dto.ClaimedTask;
import com.yuki.reactorsched.executor.sdk.dto.ExecutionResultRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisterExecutorRequest;
import com.yuki.reactorsched.executor.sdk.dto.RegisteredExecutor;
import com.yuki.reactorsched.executor.sdk.dto.RenewLeaseRequest;
import com.yuki.reactorsched.executor.sdk.dto.RenewedLease;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestClient;

import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

/**
 * The executor contract over HTTP (PRD 7, 10.3).
 *
 * <p>Every call carries the shared executor token as {@code X-ReactorSched-Token}. Error answers
 * become {@link ExecutorApiException} with the status and the machine-readable code of the body, so
 * the engine reacts to semantics instead of parsing JSON; a call that never reached the center is a
 * transport failure the engine retries on the next tick.
 *
 * <p>The JSON converter uses the application's {@link ObjectMapper}, so dates and the payload tree
 * are read exactly the way the center writes them.
 */
public class RestExecutorGateway implements ExecutorGateway {

    static final String TOKEN_HEADER = "X-ReactorSched-Token";

    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    public RestExecutorGateway(ExecutorSdkProperties properties, ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.restClient = RestClient.builder()
                .baseUrl(properties.getBaseUrl())
                .requestFactory(requestFactory(properties.getRequestTimeout()))
                .defaultHeader(TOKEN_HEADER, properties.getToken())
                // Errors become ExecutorApiException instead of the default
                // HttpClientErrorException, so status and code stay available to the engine.
                .defaultStatusHandler(new ExecutorErrorHandler())
                .messageConverters(converters -> {
                    converters.removeIf(MappingJackson2HttpMessageConverter.class::isInstance);
                    converters.add(new MappingJackson2HttpMessageConverter(objectMapper));
                })
                .build();
    }

    @Override
    public RegisteredExecutor register(RegisterExecutorRequest request) {
        return post("/api/v1/executors/register", request, RegisteredExecutor.class);
    }

    @Override
    public void heartbeat(long executorId) {
        post("/api/v1/executors/{id}/heartbeat", null, Void.class, executorId);
    }

    @Override
    public List<ClaimedTask> claim(long executorId, ClaimTasksRequest request) {
        return post("/api/v1/executors/{id}/tasks/claim", request,
                new ParameterizedTypeReference<List<ClaimedTask>>() {
                }, executorId);
    }

    @Override
    public void submitResult(long executorId, long executionId, ExecutionResultRequest request) {
        post("/api/v1/executors/{id}/executions/{executionId}/result", request, Void.class,
                executorId, executionId);
    }

    @Override
    public RenewedLease renewLease(long executorId, long executionId, RenewLeaseRequest request) {
        return post("/api/v1/executors/{id}/executions/{executionId}/renew", request, RenewedLease.class,
                executorId, executionId);
    }

    private <T> T post(String uri, Object body, Class<T> responseType, Object... uriVariables) {
        return call(restClient -> {
            RestClient.RequestBodySpec spec = restClient.post().uri(uri, uriVariables);
            if (body != null) {
                spec = spec.body(body);
            }
            if (responseType == Void.class) {
                spec.retrieve().toBodilessEntity();
                return null;
            }
            return spec.retrieve().body(responseType);
        });
    }

    private <T> T post(String uri, Object body, ParameterizedTypeReference<T> responseType, Object... uriVariables) {
        return call(restClient -> restClient.post()
                .uri(uri, uriVariables)
                .body(body)
                .retrieve()
                .body(responseType));
    }

    /**
     * One HTTP exchange with the two failure shapes an executor must survive: the center answered
     * with an error (already an {@link ExecutorApiException} from the status handler) or the call
     * never completed - connection refused, read timeout, reset.
     */
    private <T> T call(Function<RestClient, T> exchange) {
        try {
            return exchange.apply(restClient);
        } catch (ExecutorApiException e) {
            throw e;
        } catch (RuntimeException e) {
            throw ExecutorApiException.transport(describe(e));
        }
    }

    private static String describe(RuntimeException e) {
        String message = e.getMessage();
        return message == null || message.isBlank() ? e.getClass().getSimpleName() : message;
    }

    private static JdkClientHttpRequestFactory requestFactory(Duration timeout) {
        HttpClient httpClient = HttpClient.newBuilder().connectTimeout(timeout).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(httpClient);
        factory.setReadTimeout(timeout);
        return factory;
    }

    /** Turns an error answer into {@link ExecutorApiException}, keeping status, code and message. */
    private final class ExecutorErrorHandler extends DefaultResponseErrorHandler {

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            ErrorBody body = readErrorBody(response);
            throw ExecutorApiException.of(response.getStatusCode().value(),
                    body == null ? null : body.code(),
                    body == null ? null : body.message());
        }

        private ErrorBody readErrorBody(ClientHttpResponse response) {
            try {
                return objectMapper.readValue(response.getBody(), ErrorBody.class);
            } catch (IOException | RuntimeException e) {
                // A non-JSON error body (a proxy page, an empty 502) carries no code; the status
                // alone still tells the engine what to do.
                return null;
            }
        }
    }

    /** The center's error shape (AGENTS.md 11): {@code {"code": ..., "message": ...}}. */
    private record ErrorBody(String code, String message) {
    }
}
