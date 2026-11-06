package com.bnp.pf.transformation.evaproxy.service; // păstrează package-ul tău

// importurile existente: ProcessingContext, OrchestrationMessage, ApplicationService, GsonInstance, ApplicationBuilder

import com.bnp.pf.transformation.commons.api.error.CollectError;
import com.bnp.pf.transformation.commons.api.generalproxyobs.service.GeneralNullOrEmptyCheckService;
import com.bnp.pf.transformation.commons.api.message.input.Application;
import com.bnp.pf.transformation.commons.api.message.input.Customer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;
import java.util.List;

import static com.bnp.pf.transformation.commons.api.message.input.ApplicantType.COMPANY_PERSON;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
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
    private ProcessingContext processingContext;

    @BeforeEach
    void setUp() {
        application = ApplicationBuilder.createApplication();
        processingContext = new ProcessingContext();
        processingContext.setApplication(application);
        processingContext.setBusinessKey(BUSINESS_KEY);
    }

    private static Customer customer(String applicantType, LocalDateTime birthDate) {
        Customer customer = new Customer();
        customer.setApplicantType(applicantType);
        customer.setBirthDate(birthDate);
        return customer;
    }

    @Test
    void loanCheckProcess_whenLoanIsValid_shouldSaveApplicationThenCheckEva() {
        application.getLoan().setCustomers(List.of(customer(null, LocalDateTime.of(1990, 1, 1, 0, 0))));

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        ArgumentCaptor<String> applicationJson = ArgumentCaptor.forClass(String.class);
        InOrder inOrder = inOrder(applicationService, mappingService);
        inOrder.verify(applicationService).saveApplicationRequest(same(application), same(orchestrationMessage),
                applicationJson.capture(), same(processingContext));
        inOrder.verify(mappingService).checkEva(same(processingContext), same(application.getLoan()));
        assertThat(applicationJson.getValue()).isEqualTo(GsonInstance.createGson().toJson(application));
    }

    @Test
    void loanCheckProcess_whenNoCustomersAndNoCompanies_shouldSaveApplicationAndSkipWithoutErrors() {
        application.getLoan().setCustomers(List.of());
        application.getLoan().setCompanies(List.of());

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verifyNoInteractions(mappingService, collectError);
        assertThat(mockingDetails(applicationService).getInvocations()).hasSize(1);
        assertThat(processingContext.getErrorMap()).isEmpty();
    }

    @Test
    void loanCheckProcess_whenCustomerHasNoBirthDate_shouldRegister400ErrorAndNotCallEva() {
        application.getLoan().setCustomers(List.of(customer(null, null)));

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verify(collectError).addError(same(processingContext), eq("400"), contains(BUSINESS_KEY));
        verify(collectError).addErrorSignal(same(processingContext), any());
        verifyNoInteractions(mappingService);
        assertThat(processingContext.getErrorMap().values())
                .singleElement().asString()
                .startsWith("Unable to process EVA data. Birth date is missing");
    }

    @Test
    void loanCheckProcess_whenCompanyPersonHasNoBirthDate_shouldStillCheckEva() {
        application.getLoan().setCustomers(List.of(customer(COMPANY_PERSON.getApplicant(), null)));

        integrityCheckService.loanCheckProcess(processingContext, orchestrationMessage);

        verify(mappingService).checkEva(same(processingContext), same(application.getLoan()));
        verifyNoInteractions(collectError);
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
