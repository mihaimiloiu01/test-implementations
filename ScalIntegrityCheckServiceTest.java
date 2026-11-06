package com.bnp.pf.transformation.scalproxy.service; // păstrează package-ul tău

// importurile existente: ProcessingContext, OrchestrationMessage, ApplicationService, GsonInstance

import com.bnp.pf.transformation.commons.api.error.CollectError;
import com.bnp.pf.transformation.commons.api.generalproxyobs.service.GeneralNullOrEmptyCheckService;
import com.bnp.pf.transformation.commons.api.message.input.Application;
import com.bnp.pf.transformation.commons.api.message.input.Customer;
import com.bnp.pf.transformation.commons.api.message.input.Loan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class IntegrityCheckServiceTest {

    private static final String BUSINESS_KEY = "BK-001";

    @Mock
    private GeneralNullOrEmptyCheckService generalNullOrEmptyCheckService;
    @Mock
    private CollectError collectError;
    @Mock
    private ApplicationService applicationService;
    @Mock
    private MappingService mappingService;
    @Mock
    private OrchestrationMessage orchestrationMessage;

    @InjectMocks
    private IntegrityCheckService integrityCheckService;

    private Application application;
    private Loan loan;
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        loan = new Loan();
        loan.setCustomers(List.of(new Customer()));
        application = new Application();
        application.setLoan(loan);

        processingContext = new ProcessingContext();
        processingContext.setApplication(application);
        processingContext.setBusinessKey(BUSINESS_KEY);
    }

    @Test
    void loanCheckProcess_whenLoanIsValid_shouldSaveApplicationThenCheckScal() {
        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        ArgumentCaptor<String> applicationJson = ArgumentCaptor.forClass(String.class);
        InOrder inOrder = inOrder(applicationService, mappingService);
        inOrder.verify(applicationService).saveApplicationRequest(same(application), same(orchestrationMessage),
                applicationJson.capture(), same(processingContext));
        inOrder.verify(mappingService).checkScalProcess(same(processingContext), same(loan));
        assertThat(applicationJson.getValue()).isEqualTo(GsonInstance.createGson().toJson(application));
    }

    @Test
    void loanCheckProcess_whenNoCustomersAndNoCompanies_shouldSaveApplicationAndSkipWithoutErrors() {
        loan.setCustomers(List.of());
        loan.setCompanies(List.of());

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verifyNoInteractions(mappingService, collectError);
        assertThat(mockingDetails(applicationService).getInvocations()).hasSize(1);
        assertThat(processingContext.getErrorMap()).isEmpty();
    }

    @Test
    void loanCheckProcess_whenLoanIsMissing_shouldReportLoanNotPresent() {
        application.setLoan(null);

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verify(generalNullOrEmptyCheckService).loanNotPresent(processingContext);
        assertThat(mockingDetails(mappingService).getInvocations()).isEmpty();
    }

    @Test
    void loanCheckProcess_whenSaveFails_shouldRegisterErrorAndStop() {
        RuntimeException dbError = new RuntimeException("db down");
        doThrow(dbError).when(applicationService).saveApplicationRequest(any(), any(), any(), any());

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verify(collectError).addError(processingContext, dbError);
        verify(collectError).addErrorSignal(same(processingContext), any());
        verifyNoInteractions(mappingService, generalNullOrEmptyCheckService);
        assertThat(processingContext.getErrorMap().values())
                .singleElement().asString()
                .contains(BUSINESS_KEY, "db down");
    }

    @Test
    void loanCheckProcess_whenApplicationIsMissing_shouldDoNothing() {
        processingContext.setApplication(null);

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verifyNoInteractions(applicationService, mappingService, collectError);
        assertThat(processingContext.getErrorMap()).isEmpty();
    }
}
