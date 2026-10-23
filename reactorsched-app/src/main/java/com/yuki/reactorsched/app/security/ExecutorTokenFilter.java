package com.yuki.reactorsched.app.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.common.web.ApiError;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * Authenticates the executor endpoints with the shared executor token (PRD 10.3).
 *
 * <p>Registration, heartbeat, claim, result and renewal are {@code POST}s under
 * {@code /api/v1/executors/}; each must carry the configured secret as
 * {@code X-ReactorSched-Token}. {@code GET /api/v1/executors} is an open read endpoint like the
 * rest of the query API and stays untouched by this filter.
 *
 * <p>The comparison is constant-time, so a wrong token cannot be discovered byte by byte through
 * timing. The rejection body is the same {@code {code, message}} shape the REST errors use, so an
 * executor client never parses HTML to find out it is unauthorized.
 */
public class ExecutorTokenFilter extends OncePerRequestFilter {

    static final String TOKEN_HEADER = "X-ReactorSched-Token";
    private static final String EXECUTOR_PATH_PREFIX = "/api/v1/executors/";

    private final String expectedToken;
    private final ObjectMapper objectMapper;

    public ExecutorTokenFilter(String expectedToken, ObjectMapper objectMapper) {
        this.expectedToken = expectedToken;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        if (guardsExecutorWrite(request) && !tokenMatches(request)) {
            reject(response);
            return;
        }
        filterChain.doFilter(request, response);
    }

    /** Executor endpoints are writes under the executor path; everything else is not ours. */
    private static boolean guardsExecutorWrite(HttpServletRequest request) {
        return "POST".equalsIgnoreCase(request.getMethod()) && servletPath(request).startsWith(EXECUTOR_PATH_PREFIX);
    }

    private static String servletPath(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String contextPath = request.getContextPath();
        if (contextPath != null && !contextPath.isEmpty() && uri.startsWith(contextPath)) {
            return uri.substring(contextPath.length());
        }
        return uri;
    }

    private boolean tokenMatches(HttpServletRequest request) {
        String presented = request.getHeader(TOKEN_HEADER);
        if (presented == null || presented.isEmpty()) {
            return false;
        }
        return MessageDigest.isEqual(
                expectedToken.getBytes(StandardCharsets.UTF_8),
                presented.getBytes(StandardCharsets.UTF_8));
    }

    private void reject(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        objectMapper.writeValue(response.getWriter(),
                new ApiError("UNAUTHORIZED", "executor endpoints require a valid " + TOKEN_HEADER + " header"));
    }
}
