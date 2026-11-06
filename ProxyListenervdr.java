package com.bnp.pf.transformation.rvdrproxy.controller; // păstrează package-ul tău

// importurile existente: OrchestrationMessage, Signal, Error (commons, NU java.lang.Error), ErrorHandler,
// ProcessingContext, ProcessingFailedException, LocalDateTimeUtils, KafkaPublisher, IntegrityCheckService,
// GeneralNullOrEmptyCheckService, ApplicationService, CollectError, @NonNull, CORRELATION_HEADER, OK, KO

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bnp.pf.transformation.commons.api.generalproxyobs.util.ProxyObsConstant.ERROR_MESSAGE;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProxyListener {

    /** Cheia din properties pe care o citește Camunda (aceeași valoare ca în legacy). */
    private static final String STATUS = "STATUS";

    private final KafkaPublisher kafkaPublisher;
    private final IntegrityCheckService integrityCheckService;
    private final GeneralNullOrEmptyCheckService generalNullOrEmptyCheckService;
    private final ApplicationService applicationService;
    private final CollectError collectError;

    @Value("${kafka.producers[0].topic}")
    private String statusTopic;

    @Value("${kafka.producers[1].topic}")
    private String errorTopic;

    @Value("${app.version:unknown}")
    private String version;

    @Value("${proxy-name}")
    private String proxyName;

    @KafkaListener(topics = "${kafka.consumers[0].topic}",
            containerFactory = "#{kafkaListenerContainerFactory['${kafka.consumers[0].topic}']}")
    public void consume(@Payload @NonNull OrchestrationMessage orchestrationMessage,
                        @Header(name = CORRELATION_HEADER, required = false) String correlation,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String receivedTopic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int receivedPartition,
                        @Header(KafkaHeaders.OFFSET) long receivedOffset) {
        log.info("CONSUME topic = {}, partition = {}, offset = {}, correlation = {}",
                receivedTopic, receivedPartition, receivedOffset, correlation);
        log.debug("Received signal from Granting Api {}", orchestrationMessage);

        ProcessingContext processingContext = new ProcessingContext();
        processingContext.getIndex().set(1); // ca în legacy: erorile se numerotează de la 1

        try {
            // Legacy: fără correlation Camunda nu poate corela -> doar erorile pe error topic, fără status, stop
            generalNullOrEmptyCheckService.correlationEmptyCheck(processingContext, correlation, proxyName, orchestrationMessage);
            if (hasErrors(processingContext)) {
                publishErrors(processingContext, correlation, eventTime(orchestrationMessage));
                return;
            }

            // Legacy: aplicație / businessKey / flowId lipsă -> NOT_OK pe status + erori, fără apel RVDR.
            // Fără update în DB: aplicația se salvează abia în loanCheckProcess.
            generalNullOrEmptyCheckService.applicationEmptyCheck(processingContext);
            generalNullOrEmptyCheckService.businessKeyFlowIdEmptyCheck(processingContext, proxyName);
            if (hasErrors(processingContext)) {
                publishStatusAndErrors(processingContext, correlation, eventTime(orchestrationMessage));
                return;
            }

            integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

            LocalDateTime eventTime = eventTime(orchestrationMessage);
            if (hasErrors(processingContext)) {
                updateApplicationStatus(processingContext, KO);
                publishStatusAndErrors(processingContext, correlation, eventTime);
            } else {
                updateApplicationStatus(processingContext, OK);
                publishStatus(processingContext, correlation, OK, eventTime);
            }
        } catch (RuntimeException e) {
            throw new ProcessingFailedException(processingContext, e.getMessage(), e);
        }
    }

    /**
     * Legacy nu avea status în DB: o eroare aici (ex. rândul nu există pentru că save-ul a picat)
     * nu are voie să blocheze răspunsul către Camunda.
     */
    private void updateApplicationStatus(ProcessingContext processingContext, String status) {
        try {
            applicationService.updateApplicationStatus(processingContext.getBusinessKey(), status);
        } catch (RuntimeException e) {
            log.error("Could not set status {} for application {}: {}",
                    status, processingContext.getBusinessKey(), e.getMessage(), e);
        }
    }

    /** Legacy: statusul pleacă MEREU pe status topic, apoi detaliile erorilor pe error topic. */
    private void publishStatusAndErrors(ProcessingContext processingContext, String correlation, LocalDateTime eventTime) {
        publishStatus(processingContext, correlation, KO, eventTime);
        publishErrors(processingContext, correlation, eventTime);
    }

    private void publishStatus(ProcessingContext processingContext, String correlation, String status,
                               LocalDateTime eventTime) {
        Map<CharSequence, CharSequence> properties = new HashMap<>();
        properties.put(STATUS, status);

        Signal statusMessage = processingContext.getStatusMessage();
        statusMessage.setProperties(properties);
        kafkaPublisher.publishToKafka(statusTopic, correlation, statusMessage, eventTime, version);
    }

    private void publishErrors(ProcessingContext processingContext, String correlation, LocalDateTime eventTime) {
        if (CollectionUtils.isEmpty(processingContext.getErrorSignalMap())) {
            // Erori înregistrate doar în ErrorHandler (fără semnal): construim semnalul din ele, ca să nu se piardă.
            // Ca în legacy, pe error topic pleacă doar mesajul fiecărei erori.
            List<Error> errors = errorsOf(processingContext);
            log.info("No error signal built for businessKey {}, building it from errors {}",
                    processingContext.getBusinessKey(), errors);
            errors.forEach(error -> processingContext.getErrorMap().put(
                    ERROR_MESSAGE + processingContext.getIndex().getAndIncrement(), error.getErrorMessage()));
            collectError.addErrorSignal(processingContext, errorSignalOf(processingContext));
        }

        processingContext.getErrorSignalMap().values().forEach(errorSignal ->
                kafkaPublisher.publishToKafka(errorTopic, correlation, errorSignal, eventTime, version));
    }

    /** Legacy: semnalul de eroare poartă businessKey și flowId ale aplicației, ca Camunda să-l poată corela. */
    private static Signal errorSignalOf(ProcessingContext processingContext) {
        if (processingContext.getErrorMessage() != null) {
            return processingContext.getErrorMessage();
        }
        Signal errorSignal = new Signal();
        errorSignal.setBusinessKey(processingContext.getBusinessKey());
        errorSignal.setFlowId(processingContext.getFlowId());
        return errorSignal;
    }

    private static boolean hasErrors(ProcessingContext processingContext) {
        return !errorsOf(processingContext).isEmpty()
                || !CollectionUtils.isEmpty(processingContext.getErrorSignalMap());
    }

    private static List<Error> errorsOf(ProcessingContext processingContext) {
        ErrorHandler errorHandler = processingContext.getErrorHandler();
        return errorHandler == null || errorHandler.getErrors() == null ? List.of() : errorHandler.getErrors();
    }

    /** Legacy: data evenimentului din metadata, altfel momentul curent; aceeași pentru status și erori. */
    private static LocalDateTime eventTime(OrchestrationMessage orchestrationMessage) {
        return orchestrationMessage.getMetadata() != null
                ? LocalDateTimeUtils.convertToNewLocalDateTime(orchestrationMessage)
                : LocalDateTime.now();
    }
}
