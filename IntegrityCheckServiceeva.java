package com.bnp.pf.transformation.evaproxy.service; // păstrează package-ul tău

// importurile de domeniu existente: Application, Loan, Customer, OrchestrationMessage, ProcessingContext,
// CollectError, ApplicationService, GeneralNullOrEmptyCheckService, GsonInstance

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import static com.bnp.pf.transformation.commons.api.generalproxyobs.util.ProxyObsConstant.ERROR_MESSAGE;
import static com.bnp.pf.transformation.commons.api.message.input.ApplicantType.COMPANY_PERSON;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegrityCheckService {

    private final GeneralNullOrEmptyCheckService generalNullOrEmptyCheckService;
    private final CollectError collectError;
    private final ApplicationService applicationService;
    private final MappingService mappingService;
    private final Gson gson = GsonInstance.createGson();

    public void loanCheckProcess(ProcessingContext processingContext, OrchestrationMessage orchestrationMessage) {
        Application application = processingContext.getApplication();
        if (application == null) {
            // eroarea e înregistrată de generalNullOrEmptyCheckService.applicationEmptyCheck
            log.warn("No application in context for businessKey {}, skipping loan check",
                    processingContext.getBusinessKey());
            return;
        }

        // Salvăm înainte de validare, ca rândul să existe când listener-ul îl marchează OK/KO.
        if (!saveApplication(application, processingContext, orchestrationMessage)) {
            return;
        }

        Loan loan = application.getLoan();
        if (loan == null) {
            // Legacy: NPE și nu se publica nimic (timeout în Camunda). Acum: eroare -> KO.
            generalNullOrEmptyCheckService.loanNotPresent(processingContext);
            return;
        }

        if (CollectionUtils.isEmpty(loan.getCustomers()) && CollectionUtils.isEmpty(loan.getCompanies())) {
            // Legacy: MappingService doar loga "No Customers/Companies found" -> status OK, scoring-ul nu se blochează
            log.warn("No customers or companies found for businessKey {}", processingContext.getBusinessKey());
            return;
        }

        if (hasCustomerWithoutBirthDate(loan)) {
            // Legacy: NPE la construirea EvaRequest -> nu se publica nimic (timeout). Acum: eroare -> KO.
            registerValidationError(processingContext,
                    "Unable to process EVA data. Birth date is missing for a customer in Application with businessKey: "
                            + processingContext.getBusinessKey());
            return;
        }

        mappingService.checkEva(processingContext, loan);
    }

    /** Doar clienții pentru care se apelează EVA (nu U0x). Fără dată de naștere nu se poate construi EvaRequest. */
    private static boolean hasCustomerWithoutBirthDate(Loan loan) {
        return !CollectionUtils.isEmpty(loan.getCustomers()) && loan.getCustomers().stream()
                .filter(customer -> !COMPANY_PERSON.getApplicant().equals(customer.getApplicantType()))
                .anyMatch(customer -> customer.getBirthDate() == null);
    }

    private boolean saveApplication(Application application,
                                    ProcessingContext processingContext,
                                    OrchestrationMessage orchestrationMessage) {
        try {
            String applicationJson = gson.toJson(application);
            applicationService.saveApplicationRequest(application, orchestrationMessage, applicationJson, processingContext);
            return true;
        } catch (Exception e) {
            String message = "Error occurred when trying to save application to database. Business Key "
                    + processingContext.getBusinessKey() + ", flow ID " + processingContext.getFlowId()
                    + ": " + e.getMessage();
            log.error(message, e);
            collectError.addError(processingContext, e);
            registerErrorSignal(processingContext, message);
            return false;
        }
    }

    private void registerValidationError(ProcessingContext processingContext, String message) {
        log.error(message);
        collectError.addError(processingContext, String.valueOf(HttpStatus.BAD_REQUEST.value()), message);
        registerErrorSignal(processingContext, message);
    }

    private void registerErrorSignal(ProcessingContext processingContext, String message) {
        processingContext.getErrorMap().put(ERROR_MESSAGE + processingContext.getIndex().getAndIncrement(), message);
        collectError.addErrorSignal(processingContext, processingContext.getErrorMessage());
    }
}
