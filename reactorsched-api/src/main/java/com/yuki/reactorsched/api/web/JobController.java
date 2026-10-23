package com.yuki.reactorsched.api.web;

import com.yuki.reactorsched.api.dto.JobAdminRequest;
import com.yuki.reactorsched.api.dto.JobRequest;
import com.yuki.reactorsched.api.dto.TriggerJobRequest;
import com.yuki.reactorsched.common.web.PageLimits;
import com.yuki.reactorsched.core.backpressure.BackpressureGuard;
import com.yuki.reactorsched.core.domain.JobDefinition;
import com.yuki.reactorsched.core.domain.JobStatus;
import com.yuki.reactorsched.core.domain.Task;
import com.yuki.reactorsched.core.dto.JobPage;
import com.yuki.reactorsched.core.dto.JobQuery;
import com.yuki.reactorsched.core.dto.JobView;
import com.yuki.reactorsched.core.dto.TaskView;
import com.yuki.reactorsched.core.service.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.Valid;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Job definition API (PRD 5.2, 10.3).
 *
 * <p>A definition says what to run and when; it never says whether the work succeeded. Reads are
 * open, every write needs the ADMIN role (see {@code SecurityConfig}) and carries an operator and a
 * reason so {@code operation_audit} can explain it later.
 *
 * <p>{@code PUT /jobs/{id}} is a partial edit: omitted fields keep their value, but the operator is
 * still mandatory and travels as a query parameter because the body is the definition itself.
 */
@Profile("!executor")
@RestController
@RequestMapping("/api/v1/jobs")
public class JobController {

    private final JobService jobService;
    private final ObjectMapper objectMapper;
    private final BackpressureGuard backpressureGuard;

    public JobController(JobService jobService, ObjectMapper objectMapper, BackpressureGuard backpressureGuard) {
        this.jobService = jobService;
        this.objectMapper = objectMapper;
        this.backpressureGuard = backpressureGuard;
    }

    @PostMapping
    public ResponseEntity<JobView> create(@RequestParam String operator,
                                          @Valid @RequestBody JobRequest request) {
        JobDefinition created = jobService.create(request.toCommand(), operator);
        return ResponseEntity.status(HttpStatus.CREATED).body(JobView.from(created, objectMapper));
    }

    @PutMapping("/{id}")
    public JobView update(@PathVariable long id,
                          @RequestParam String operator,
                          @Valid @RequestBody JobRequest request) {
        return JobView.from(jobService.update(id, request.toCommand(), operator), objectMapper);
    }

    @GetMapping("/{id}")
    public JobView get(@PathVariable long id) {
        return JobView.from(jobService.get(id), objectMapper);
    }

    @GetMapping
    public JobPage list(@RequestParam(required = false) JobStatus status,
                        @RequestParam(required = false)
                        @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                        @RequestParam(required = false)
                        @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
                        @RequestParam(defaultValue = "0") int page,
                        @RequestParam(defaultValue = "20") int size) {
        int safeSize = Math.max(1, Math.min(size, PageLimits.MAX_PAGE_SIZE));
        int safePage = Math.max(0, page);
        if (from != null && to != null && !from.isBefore(to)) {
            throw new IllegalArgumentException("from must be before to");
        }
        return jobService.query(new JobQuery(status, from, to, safePage, safeSize));
    }

    @PostMapping("/{id}/pause")
    public JobView pause(@PathVariable long id, @Valid @RequestBody JobAdminRequest request) {
        return JobView.from(jobService.pause(id, request.operator(), request.reason()), objectMapper);
    }

    @PostMapping("/{id}/enable")
    public JobView enable(@PathVariable long id, @Valid @RequestBody JobAdminRequest request) {
        return JobView.from(jobService.enable(id, request.operator(), request.reason()), objectMapper);
    }

    @DeleteMapping("/{id}")
    public JobView delete(@PathVariable long id, @Valid @RequestBody JobAdminRequest request) {
        return JobView.from(jobService.delete(id, request.operator(), request.reason()), objectMapper);
    }

    /**
     * Runs the definition once, right now, outside its schedule. The response is the instance that was
     * created (201) or the one the same client key created earlier (200). Backpressure (PRD 8.5)
     * answers 429 before the service call, outside any business transaction.
     */
    @PostMapping("/{id}/trigger")
    public ResponseEntity<TriggerJobResponse> trigger(@PathVariable long id,
                                                      @Valid @RequestBody TriggerJobRequest request) {
        backpressureGuard.checkAdmission("manual trigger of job " + id);
        JobService.ManualTrigger result = jobService.trigger(id, request.clientKey(), request.operator(), request.reason());
        HttpStatus status = result.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status)
                .body(new TriggerJobResponse(TaskView.from(result.instance(), objectMapper), result.created()));
    }

    /** Instances created for a definition, newest first. */
    @GetMapping("/{id}/instances")
    public List<TaskView> instances(@PathVariable long id,
                                    @RequestParam(defaultValue = "50") int size) {
        List<Task> instances = jobService.instances(id, size);
        return instances.stream().map(task -> TaskView.from(task, objectMapper)).toList();
    }

    /** Response of a manual trigger: the instance and whether this call is what created it. */
    public record TriggerJobResponse(TaskView instance, boolean created) {
    }
}
