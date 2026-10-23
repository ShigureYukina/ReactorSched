package com.yuki.reactorsched.app.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yuki.reactorsched.executor.registry.properties.ExecutorProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.http.HttpMethod;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.factory.PasswordEncoderFactories;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Authentication and authorization for the management endpoints (PRD 8.4, phase 5).
 *
 * <p>Cancel, replay and fault injection are operational writes: they require a configured user
 * with {@code ROLE_ADMIN} via HTTP Basic. Everything else (task submission, queries, executions,
 * timeline, decisions, metrics, experiment records, the static panel and the actuator read
 * endpoints) stays open - PRD 8.4 only mandates protecting the management interfaces.
 *
 * <p>Rejections use the same {@code {code, message}} JSON shape as the REST error responses, so
 * clients never receive an HTML login page. Passwords come from configuration and are encoded in
 * memory at startup; the development default must be overridden outside a local machine.
 *
 * <p>The executor endpoints ({@code POST /api/v1/executors/**}, PRD 10.3) are authenticated by
 * {@link ExecutorTokenFilter} with the shared executor token instead of a user: they are called by
 * processes, not people.
 */
@Profile("!executor")
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    private final SecurityProperties properties;
    private final ExecutorProperties executorProperties;
    private final ObjectMapper objectMapper;

    public SecurityConfig(SecurityProperties properties, ExecutorProperties executorProperties,
                          ObjectMapper objectMapper) {
        this.properties = properties;
        this.executorProperties = executorProperties;
        this.objectMapper = objectMapper;
    }

    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        // One entry point for both rejection paths: a wrong password fails inside the basic auth
        // filter (httpBasic), a missing one fails later in the exception handling filter.
        RestAuthenticationEntryPoint entryPoint = new RestAuthenticationEntryPoint(objectMapper);
        http
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session -> session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .httpBasic(httpBasic -> httpBasic.authenticationEntryPoint(entryPoint))
                .exceptionHandling(handling -> handling
                        .authenticationEntryPoint(entryPoint)
                        .accessDeniedHandler(new RestAccessDeniedHandler(objectMapper)))
                .addFilterBefore(new ExecutorTokenFilter(executorProperties.getToken(), objectMapper),
                        UsernamePasswordAuthenticationFilter.class)
                .authorizeHttpRequests(requests -> requests
                        // Job definitions are the scheduler's configuration: every write is ADMIN,
                        // every read is open so the management UI can render without credentials.
                        .requestMatchers(HttpMethod.POST, "/api/v1/jobs", "/api/v1/jobs/*/pause",
                                "/api/v1/jobs/*/enable", "/api/v1/jobs/*/trigger").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.PUT, "/api/v1/jobs/*").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.DELETE, "/api/v1/jobs/*").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.POST, "/api/v1/tasks/*/cancel").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.POST, "/api/v1/tasks/*/replay").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.PUT, "/api/v1/fault-lab/downstream").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.POST, "/api/v1/fault-lab/executor/*/crash",
                                "/api/v1/fault-lab/executions/*/duplicate-result",
                                "/api/v1/fault-lab/database").hasRole("ADMIN")
                        .anyRequest().permitAll());
        return http.build();
    }

    @Bean
    public UserDetailsService userDetailsService(PasswordEncoder encoder) {
        List<UserDetails> users = new ArrayList<>();
        properties.getUsers().forEach((username, user) -> {
            if (user.password() == null || user.password().isBlank()) {
                throw new IllegalStateException(
                        "reactorsched.security.users." + username + ".password must not be blank");
            }
            users.add(User.withUsername(username)
                    .password(encoder.encode(user.password()))
                    .roles(user.roles().toArray(String[]::new))
                    .build());
        });
        return new InMemoryUserDetailsManager(users);
    }

    @Bean
    public PasswordEncoder passwordEncoder() {
        return PasswordEncoderFactories.createDelegatingPasswordEncoder();
    }
}
