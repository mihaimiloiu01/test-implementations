package com.bnp.pf.transformation.rvdrproxy.controller; // păstrează package-ul tău

// importurile existente: OrchestrationMessage, Signal, Metadata, ProcessingContext, ProcessingFailedException,
// LocalDateTimeUtils, KafkaPublisher, IntegrityCheckService, ApplicationService

import com.bnp.pf.transformation.commons.api.error.CollectError;
import com.bnp.pf.transformation.commons.api.error.Error;
import com.bnp.pf.transformation.commons.api.error.ErrorHandler;
import com.bnp.pf.transformation.commons.api.generalproxyobs.service.GeneralNullOrEmptyCheckService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockingDetails;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;
import org.springframework.test.util.ReflectionTestUtils;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.bnp.pf.transformation.commons.api.generalproxyobs.util.ProxyObsConstant.ERROR_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class ProxyListenerTest {

    private static final String STATUS_TOPIC = "status-topic";
    private static final String ERROR_TOPIC = "error-topic";
    private static final String VERSION = "1.0.0";
    private static final String PROXY_NAME = "RVDR";
    private static final String CORRELATION = "corr-123";
    private static final String BUSINESS_KEY = "BK-001";
    private static final String FLOW_ID = "FLOW-1";
    private static final String STATUS = "STATUS";
    private static final String OK = "OK";
    private static final String KO = "KO";

    private static final LocalDateTime EVENT_TIME = LocalDateTime.of(2026, 1, 15, 10, 30);
    /** Signal e Avro: equals compară conținutul, deci două new Signal() sunt "egale". Comparăm pe referință. */
    private static final Comparator<Signal> BY_IDENTITY = (a, b) -> a == b ? 0 : 1;

    @Mock
    private KafkaPublisher kafkaPublisher;
    @Mock
    private IntegrityCheckService integrityCheckService;
    @Mock
    private GeneralNullOrEmptyCheckService generalNullOrEmptyCheckService;
    @Mock
    private ApplicationService applicationService;
    @Mock
    private CollectError collectError;

    @InjectMocks
    private ProxyListener proxyListener;

    private OrchestrationMessage orchestrationMessage;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(proxyListener, "statusTopic", STATUS_TOPIC);
        ReflectionTestUtils.setField(proxyListener, "errorTopic", ERROR_TOPIC);
        ReflectionTestUtils.setField(proxyListener, "version", VERSION);
        ReflectionTestUtils.setField(proxyListener, "proxyName", PROXY_NAME);

        orchestrationMessage = OrchestrationMessage.newBuilder()
                .setData(new Signal())
                .setMetadata(new Metadata())
                .build();
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    /** ProcessingContext e creat în consume(): îl populăm când ajunge la loanCheckProcess, ca un IntegrityCheckService real. */
    private void givenContextAfterIntegrityCheck(Signal statusMessage, List<Error> errors, Map<String, Signal> errorSignalMap) {
        doAnswer(invocation -> {
            ProcessingContext processingContext = invocation.getArgument(0);
            ErrorHandler errorHandler = new ErrorHandler();
            errorHandler.setErrors(errors);
            processingContext.setBusinessKey(BUSINESS_KEY);
            processingContext.setFlowId(FLOW_ID);
            processingContext.setStatusMessage(statusMessage);
            processingContext.setErrorHandler(errorHandler);
            processingContext.setErrorSignalMap(errorSignalMap);
            return null;
        }).when(integrityCheckService).loanCheckProcess(any(ProcessingContext.class), same(orchestrationMessage));
    }

    /** O verificare din commons care înregistrează o eroare și semnalul ei. */
    private static Answer<Void> addingError(Signal errorSignal) {
        return invocation -> {
            ProcessingContext processingContext = invocation.getArgument(0);
            ErrorHandler errorHandler = new ErrorHandler();
            errorHandler.setErrors(List.of(new Error("400", "Missing data")));
            processingContext.setErrorHandler(errorHandler);
            processingContext.getErrorSignalMap().put(BUSINESS_KEY, errorSignal);
            return null;
        };
    }

    private void consume(String correlation) {
        proxyListener.consume(orchestrationMessage, correlation, "granting-topic", 0, 42L);
    }

    /** Pentru căile care publică: data evenimentului vine din metadata. */
    private void consumeWithFixedEventTime(String correlation) {
        try (MockedStatic<LocalDateTimeUtils> utils = mockStatic(LocalDateTimeUtils.class)) {
            utils.when(() -> LocalDateTimeUtils.convertToNewLocalDateTime(orchestrationMessage)).thenReturn(EVENT_TIME);
            consume(correlation);
        }
    }

    private static Map<String, Signal> signalMap(Signal signal) {
        Map<String, Signal> map = new HashMap<>();
        map.put(BUSINESS_KEY, signal);
        return map;
    }

    /** Argumentele tuturor apelurilor publishToKafka, în ordinea apelurilor. */
    private static final class PublishCalls {
        final ArgumentCaptor<String> topic = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<String> correlation = ArgumentCaptor.forClass(String.class);
        final ArgumentCaptor<Signal> signal = ArgumentCaptor.forClass(Signal.class);
        final ArgumentCaptor<LocalDateTime> eventTime = ArgumentCaptor.forClass(LocalDateTime.class);
        final ArgumentCaptor<String> version = ArgumentCaptor.forClass(String.class);
    }

    /** Primește mock-ul deja trecut prin verify(...): numărul de apeluri e cel ales acolo. */
    private static PublishCalls capturePublish(KafkaPublisher verification) {
        PublishCalls calls = new PublishCalls();
        verification.publishToKafka(calls.topic.capture(), calls.correlation.capture(), calls.signal.capture(),
                calls.eventTime.capture(), calls.version.capture());
        return calls;
    }

    /** Statusul scris în DB. verify implicit = exact un update. */
    private String statusSetFor(String businessKey) {
        ArgumentCaptor<String> status = ArgumentCaptor.forClass(String.class);
        verify(applicationService).updateApplicationStatus(eq(businessKey), status.capture());
        return status.getValue();
    }

    private static List<String> invokedMethods(MockingDetails mock) {
        return mock.getInvocations().stream()
                .map(invocation -> invocation.getMethod().getName())
                .toList();
    }

    // =====================================================================================
    // Tests
    // =====================================================================================

    @Test
    void consume_whenNoErrors_shouldSetStatusOkAndPublishOnlyStatus() {
        Signal statusMessage = new Signal();
        givenContextAfterIntegrityCheck(statusMessage, List.of(), new HashMap<>());

        consumeWithFixedEventTime(CORRELATION);

        ArgumentCaptor<ProcessingContext> ctx = ArgumentCaptor.forClass(ProcessingContext.class);
        InOrder inOrder = inOrder(generalNullOrEmptyCheckService, integrityCheckService, applicationService, kafkaPublisher);
        inOrder.verify(generalNullOrEmptyCheckService)
                .correlationEmptyCheck(ctx.capture(), eq(CORRELATION), eq(PROXY_NAME), same(orchestrationMessage));
        inOrder.verify(generalNullOrEmptyCheckService).applicationEmptyCheck(ctx.capture());
        inOrder.verify(generalNullOrEmptyCheckService).businessKeyFlowIdEmptyCheck(ctx.capture(), eq(PROXY_NAME));
        inOrder.verify(integrityCheckService).loanCheckProcess(ctx.capture(), same(orchestrationMessage));
        inOrder.verify(applicationService).updateApplicationStatus(BUSINESS_KEY, OK);
        PublishCalls published = capturePublish(inOrder.verify(kafkaPublisher));

        ProcessingContext context = ctx.getValue();
        assertThat(ctx.getAllValues()).hasSize(4).allSatisfy(c -> assertThat(c).isSameAs(context));
        assertThat(mockingDetails(kafkaPublisher).getInvocations()).hasSize(1);
        assertThat(published.topic.getValue()).isEqualTo(STATUS_TOPIC);
        assertThat(published.correlation.getValue()).isEqualTo(CORRELATION);
        assertThat(published.signal.getValue()).isSameAs(statusMessage);
        assertThat(published.eventTime.getValue()).isEqualTo(EVENT_TIME);
        assertThat(published.version.getValue()).isEqualTo(VERSION);
        assertThat(statusMessage.getProperties()).containsOnly(entry(STATUS, OK));
    }

    @Test
    void consume_whenErrorsExist_shouldSetStatusKoAndPublishStatusThenErrors() {
        Signal statusMessage = new Signal();
        Signal errorSignal = new Signal();
        givenContextAfterIntegrityCheck(statusMessage, List.of(new Error("502", "SOAP Call Error")), signalMap(errorSignal));

        consumeWithFixedEventTime(CORRELATION);

        PublishCalls published = capturePublish(verify(kafkaPublisher, times(2)));

        assertThat(statusSetFor(BUSINESS_KEY)).isEqualTo(KO);
        assertThat(published.topic.getAllValues()).containsExactly(STATUS_TOPIC, ERROR_TOPIC);
        assertThat(published.signal.getAllValues())
                .usingElementComparator(BY_IDENTITY)
                .containsExactly(statusMessage, errorSignal);
        assertThat(published.correlation.getAllValues()).containsOnly(CORRELATION);
        assertThat(published.eventTime.getAllValues()).containsOnly(EVENT_TIME);
        assertThat(statusMessage.getProperties()).containsOnly(entry(STATUS, KO));
    }

    @Test
    void consume_whenOnlyErrorSignalsExist_shouldSetStatusKoAndPublishStatusThenErrors() {
        Signal errorSignal = new Signal();
        givenContextAfterIntegrityCheck(new Signal(), List.of(), signalMap(errorSignal));

        consumeWithFixedEventTime(CORRELATION);

        PublishCalls published = capturePublish(verify(kafkaPublisher, times(2)));

        assertThat(statusSetFor(BUSINESS_KEY)).isEqualTo(KO);
        assertThat(published.topic.getAllValues()).containsExactly(STATUS_TOPIC, ERROR_TOPIC);
        assertThat(published.signal.getAllValues().get(1)).isSameAs(errorSignal);
    }

    @Test
    void consume_whenErrorsButNoSignal_shouldBuildSignalWithBusinessKeyAndFlowIdAndPublishIt() {
        Signal statusMessage = new Signal();
        givenContextAfterIntegrityCheck(statusMessage, List.of(new Error("400", "Application Loan is empty")), new HashMap<>());
        // ca CollectError real: semnalul primit intră în errorSignalMap
        doAnswer(invocation -> {
            ProcessingContext processingContext = invocation.getArgument(0);
            Signal errorSignal = invocation.getArgument(1);
            processingContext.getErrorSignalMap().put(BUSINESS_KEY, errorSignal);
            return null;
        }).when(collectError).addErrorSignal(any(ProcessingContext.class), any(Signal.class));

        consumeWithFixedEventTime(CORRELATION);

        ArgumentCaptor<ProcessingContext> ctx = ArgumentCaptor.forClass(ProcessingContext.class);
        verify(collectError).addErrorSignal(ctx.capture(), any(Signal.class));
        PublishCalls published = capturePublish(verify(kafkaPublisher, times(2)));
        Signal errorSignal = published.signal.getAllValues().get(1);

        assertThat(statusSetFor(BUSINESS_KEY)).isEqualTo(KO);
        assertThat(ctx.getValue().getErrorMap()).containsOnly(entry(ERROR_MESSAGE + 1, "Application Loan is empty"));
        assertThat(published.topic.getAllValues()).containsExactly(STATUS_TOPIC, ERROR_TOPIC);
        assertThat(published.signal.getAllValues().get(0)).isSameAs(statusMessage);
        assertThat(errorSignal.getBusinessKey()).hasToString(BUSINESS_KEY);
        assertThat(errorSignal.getFlowId()).hasToString(FLOW_ID);
    }

    @Test
    void consume_whenStatusUpdateFails_shouldStillPublishStatusAndErrors() {
        Signal errorSignal = new Signal();
        givenContextAfterIntegrityCheck(new Signal(), List.of(new Error("502", "SOAP Call Error")), signalMap(errorSignal));
        doThrow(new RuntimeException("db down")).when(applicationService).updateApplicationStatus(BUSINESS_KEY, KO);

        Throwable thrown = catchThrowable(() -> consumeWithFixedEventTime(CORRELATION));

        PublishCalls published = capturePublish(verify(kafkaPublisher, times(2)));

        assertThat(thrown).isNull();
        assertThat(published.topic.getAllValues()).containsExactly(STATUS_TOPIC, ERROR_TOPIC);
        assertThat(published.signal.getAllValues().get(1)).isSameAs(errorSignal);
    }

    @Test
    void consume_whenCorrelationCheckFails_shouldPublishOnlyErrorsAndStop() {
        Signal errorSignal = new Signal();
        doAnswer(addingError(errorSignal)).when(generalNullOrEmptyCheckService)
                .correlationEmptyCheck(any(ProcessingContext.class), isNull(), eq(PROXY_NAME), same(orchestrationMessage));

        consumeWithFixedEventTime(null);

        PublishCalls published = capturePublish(verify(kafkaPublisher));
        verifyNoInteractions(integrityCheckService, applicationService);

        assertThat(invokedMethods(mockingDetails(generalNullOrEmptyCheckService))).containsExactly("correlationEmptyCheck");
        assertThat(published.topic.getValue()).isEqualTo(ERROR_TOPIC);
        assertThat(published.correlation.getValue()).isNull();
        assertThat(published.signal.getValue()).isSameAs(errorSignal);
        assertThat(published.eventTime.getValue()).isEqualTo(EVENT_TIME);
    }

    @Test
    void consume_whenBusinessKeyOrFlowIdMissing_shouldPublishStatusKoAndErrorsWithoutCallingRvdr() {
        Signal errorSignal = new Signal();
        doAnswer(addingError(errorSignal)).when(generalNullOrEmptyCheckService)
                .businessKeyFlowIdEmptyCheck(any(ProcessingContext.class), eq(PROXY_NAME));

        consumeWithFixedEventTime(CORRELATION);

        PublishCalls published = capturePublish(verify(kafkaPublisher, times(2)));
        verifyNoInteractions(integrityCheckService, applicationService);

        assertThat(published.topic.getAllValues()).containsExactly(STATUS_TOPIC, ERROR_TOPIC);
        assertThat(published.signal.getAllValues().get(0).getProperties()).containsOnly(entry(STATUS, KO));
        assertThat(published.signal.getAllValues().get(1)).isSameAs(errorSignal);
        assertThat(published.eventTime.getAllValues()).containsOnly(EVENT_TIME);
    }

    @Test
    void consume_whenIntegrityCheckThrows_shouldWrapInProcessingFailedExceptionAndPublishNothing() {
        RuntimeException cause = new RuntimeException("Error Occurred");
        doThrow(cause).when(integrityCheckService).loanCheckProcess(any(ProcessingContext.class), same(orchestrationMessage));

        Throwable thrown = catchThrowable(() -> consume(CORRELATION));

        verifyNoInteractions(applicationService, kafkaPublisher);
        assertThat(thrown)
                .isInstanceOf(ProcessingFailedException.class)
                .hasMessage("Error Occurred")
                .hasCause(cause);
    }

    @Test
    void consume_whenApplicationEmptyCheckThrows_shouldStopBeforeIntegrityCheck() {
        doThrow(new RuntimeException("application missing"))
                .when(generalNullOrEmptyCheckService).applicationEmptyCheck(any(ProcessingContext.class));

        Throwable thrown = catchThrowable(() -> consume(CORRELATION));

        verifyNoInteractions(integrityCheckService, applicationService, kafkaPublisher);
        assertThat(invokedMethods(mockingDetails(generalNullOrEmptyCheckService)))
                .containsExactly("correlationEmptyCheck", "applicationEmptyCheck");
        assertThat(thrown).isInstanceOf(ProcessingFailedException.class);
    }
}
