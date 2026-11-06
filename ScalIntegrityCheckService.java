package com.bnp.pf.transformation.scalproxy.service;

// importurile de domeniu existente: Application, Loan, OrchestrationMessage, ProcessingContext,
// CollectError, ApplicationService, GeneralNullOrEmptyCheckService, GsonInstance

import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.Objects;

import static com.bnp.pf.transformation.commons.api.generalproxyobs.util.ProxyObsConstant.ERROR_MESSAGE;

@Slf4j
@Component
@RequiredArgsConstructor
public class IntegrityCheckService {

    private final GeneralNullOrEmptyCheckService generalNullOrEmptyCheckService;
    private final CollectError collectError;
    private final ApplicationService applicationService;
    private final MappingService mappingService;
    private final Gson gson = GsonInstance.createGson();

    // Fără @Data și fără stare: bean-ul e singleton, toată starea stă în ProcessingContext.

    public void loanCheckProcess(ProcessingContext processingContext, OrchestrationMessage orchestrationMessage) {
        Application application = processingContext.getApplication();
        if (Objects.isNull(application)) {
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
        if (Objects.isNull(loan)) {
            // Legacy: "Application Loan is empty" -> NOT_OK
            generalNullOrEmptyCheckService.loanNotPresent(processingContext);
            return;
        }

        if (CollectionUtils.isEmpty(loan.getCustomers()) && CollectionUtils.isEmpty(loan.getCompanies())) {
            // Legacy: fără customers și companies -> OK, ca să nu blocheze scoring-ul
            log.warn("There are no customers or companies in the application {}", processingContext.getBusinessKey());
            return;
        }

        mappingService.checkScalProcess(processingContext, loan);
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
            processingContext.getErrorMap().put(ERROR_MESSAGE + processingContext.getIndex().getAndIncrement(), message);
            collectError.addErrorSignal(processingContext, processingContext.getErrorMessage());
            return false;
        }
    }
}
