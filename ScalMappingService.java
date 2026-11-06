package com.bnp.pf.transformation.scalproxy.service;

// importurile existente: ModelMapper, CachingService, ScalRestExternalClient, ScalApiException, BuildDTOsUtility,
// LocalDateTimeUtils, CheckEntityRepository, CheckPersonRepository, CheckPersonInitialRepository, CheckEntity,
// CheckPersonEntity, CheckPersonInitialEntity, EntityDto, PersonDto, PersonInitialDto, CheckPersonQueryDto,
// CheckPersonInitialQueryDto, CheckResultDto, GeneralProxyObsDto, GeneralProxyObsCacheDto, ProcessingContext,
// CollectError, Loan, Customer, Organisation
// Error trebuie importat explicit, altfel se ia java.lang.Error

import com.bnp.pf.transformation.commons.api.error.CollectError;
import com.bnp.pf.transformation.commons.api.error.Error;
import com.bnp.pf.transformation.commons.api.error.ErrorHandler;
import com.google.gson.Gson;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.bnp.pf.transformation.commons.api.generalproxyobs.util.ProxyObsConstant.ERROR_MESSAGE;

@Service
@Slf4j
@RequiredArgsConstructor
public class MappingService {

    private static final String CHECK_ENTITY = "checkEntity";
    private static final String CHECK_PERSON = "checkPerson";
    private static final String CHECK_PERSON_INITIAL = "checkPersonInitial";

    /** Ca în legacy: 502 pentru erorile întoarse de SCAL (în legacy, SOAP fault), 555 pentru orice altă excepție. */
    private static final String SCAL_ERROR_CODE = "502";
    private static final String TECHNICAL_ERROR_CODE = "555";
    private static final String EMPTY_RESPONSE_MESSAGE = "SCAL External API call returned an error or empty response";

    // Services
    private final ModelMapper modelMapper;
    private final CachingService cachingService;

    // REST Client
    private final ScalRestExternalClient scalRestExternalClient;
    // GSON parser
    private final Gson gson;

    // Repositories
    private final CheckPersonRepository checkPersonRepository;
    private final CheckPersonInitialRepository checkPersonInitialRepository;
    private final CheckEntityRepository checkEntityRepository;

    // Error Handling
    private final CollectError collectError;

    // Entering point from IntegrityCheckService. Ca în legacy: întâi companiile, apoi persoanele.
    public void checkScalProcess(ProcessingContext processingContext, Loan loan) {
        for (Organisation organisation : nullSafe(loan.getCompanies())) {
            checkEntity(processingContext, organisation);
        }
        for (Customer customer : nullSafe(loan.getCustomers())) {
            // Legacy: fără prenume -> verificare pe inițiale
            if (Objects.isNull(customer.getFirstName())) {
                checkPersonInitial(processingContext, customer);
            } else {
                checkPerson(processingContext, customer);
            }
        }
    }

    // Perform Checks: legacy salva MEREU check-ul, cu erorile DOAR ale lui (și când apelul pica)
    private void checkEntity(ProcessingContext processingContext, Organisation organisation) {
        EntityDto entity = modelMapper.map(organisation, EntityDto.class);
        CheckOutcome outcome = checkWithCache(processingContext, CHECK_ENTITY, gson.toJson(entity),
                () -> scalRestExternalClient.checkEntity(entity));

        GeneralProxyObsDto checkEntityDto = BuildDTOsUtility.buildCheckEntityDto(processingContext.getBusinessKey(),
                processingContext.getFlowId(), entity, outcome.responseJson(), outcome.errorHandler(), gson,
                outcome.checkDate());
        checkEntityRepository.save(modelMapper.map(checkEntityDto, CheckEntity.class));
        logSaved("CHECK_ENTITY", processingContext);
    }

    private void checkPerson(ProcessingContext processingContext, Customer customer) {
        PersonDto person = modelMapper.map(customer, PersonDto.class);
        CheckPersonQueryDto checkPersonQueryDto = new CheckPersonQueryDto();
        checkPersonQueryDto.setPerson1(person);
        CheckOutcome outcome = checkWithCache(processingContext, CHECK_PERSON, gson.toJson(person),
                () -> scalRestExternalClient.checkPerson(checkPersonQueryDto));

        GeneralProxyObsDto checkPersonDto = BuildDTOsUtility.buildCheckPersonDto(processingContext.getBusinessKey(),
                processingContext.getFlowId(), person, outcome.responseJson(), outcome.errorHandler(), gson,
                outcome.checkDate());
        checkPersonRepository.save(modelMapper.map(checkPersonDto, CheckPersonEntity.class));
        logSaved("CHECK_PERSON", processingContext);
    }

    private void checkPersonInitial(ProcessingContext processingContext, Customer customer) {
        PersonInitialDto personInitial = modelMapper.map(customer, PersonInitialDto.class);
        CheckPersonInitialQueryDto checkPersonInitialQueryDto = new CheckPersonInitialQueryDto();
        checkPersonInitialQueryDto.setPerson1(personInitial);
        CheckOutcome outcome = checkWithCache(processingContext, CHECK_PERSON_INITIAL, gson.toJson(personInitial),
                () -> scalRestExternalClient.checkPersonInitial(checkPersonInitialQueryDto));

        GeneralProxyObsDto checkPersonInitialDto = BuildDTOsUtility.buildCheckPersonInitialDto(
                processingContext.getBusinessKey(), processingContext.getFlowId(), personInitial,
                outcome.responseJson(), outcome.errorHandler(), gson, outcome.checkDate());
        checkPersonInitialRepository.save(modelMapper.map(checkPersonInitialDto, CheckPersonInitialEntity.class));
        logSaved("CHECK_PERSON_INITIAL", processingContext);
    }

    /** Cache, altfel apel SCAL. În cache intră doar răspunsurile fără erori (legacy: doar cele cu răspuns). */
    private CheckOutcome checkWithCache(ProcessingContext processingContext, String checkName, String requestJson,
                                        Supplier<CheckResultDto> scalCall) {
        String businessKey = processingContext.getBusinessKey();
        log.info("Entering into {} step for businessKey {}", checkName, businessKey);
        log.debug("Request for {} with businessKey {} is {}", checkName, businessKey, requestJson);

        Optional<GeneralProxyObsCacheDto> cached = findInCache(requestJson, processingContext.getProducer());
        if (cached.isPresent()) {
            log.info("Information found in cache for {} with businessKey {}, extracting response from cache",
                    checkName, businessKey);
            GeneralProxyObsCacheDto cacheDto = cached.get();
            // fără dată în cache -> azi; altfel NPE la construirea DTO-ului de salvat
            LocalDate checkDate = Objects.requireNonNullElseGet(cacheDto.getCreatedDate(),
                    LocalDateTimeUtils::parseLocalDateNow);
            return new CheckOutcome(cacheDto.getResponseText(), List.of(), checkDate);
        }

        CheckOutcome outcome = callScal(processingContext, checkName, scalCall);
        if (outcome.errors().isEmpty()) {
            saveInCache(requestJson, outcome.responseJson(), processingContext.getProducer());
        }
        return outcome;
    }

    // REST CALLS: nu aruncă. Orice eroare e înregistrată în context și salvată doar pe check-ul ei.
    private CheckOutcome callScal(ProcessingContext processingContext, String checkName, Supplier<CheckResultDto> scalCall) {
        String businessKey = processingContext.getBusinessKey();
        try {
            CheckResultDto response = scalCall.get();
            if (Objects.isNull(response)) {
                log.error("{} REST call returned an empty response for businessKey {}", checkName, businessKey);
                return registerError(processingContext, SCAL_ERROR_CODE, EMPTY_RESPONSE_MESSAGE);
            }

            String responseJson = gson.toJson(response);
            log.debug("Response received for {} step for businessKey {}: {}", checkName, businessKey, responseJson);
            return new CheckOutcome(responseJson, List.of(), LocalDateTimeUtils.parseLocalDateNow());
        } catch (ScalApiException e) {
            log.error("{} REST call failed for businessKey {}, status {}, error message: {}",
                    checkName, businessKey, e.getStatusCode(), e.getMessage(), e);
            return registerError(processingContext, SCAL_ERROR_CODE, messageOf(e));
        } catch (Exception e) {
            log.error("Error when executing {} external REST call for businessKey {}, error message: {}",
                    checkName, businessKey, e.getMessage(), e);
            return registerError(processingContext, TECHNICAL_ERROR_CODE, messageOf(e));
        }
    }

    /** O eroare: ErrorHandler (status KO) + errorMap (pe error topic, ca în legacy doar mesajul) + semnal de eroare. */
    private CheckOutcome registerError(ProcessingContext processingContext, String code, String message) {
        collectError.addError(processingContext, code, message);
        processingContext.getErrorMap().put(ERROR_MESSAGE + processingContext.getIndex().getAndIncrement(), message);
        collectError.addErrorSignal(processingContext, processingContext.getErrorMessage());
        return new CheckOutcome(errorJson(code, message), List.of(new Error(code, message)),
                LocalDateTimeUtils.parseLocalDateNow());
    }

    // CACHE: cheia e hash-ul JSON-ului; la hit comparăm și request-ul, ca o coliziune să nu dea datele altui client
    private Optional<GeneralProxyObsCacheDto> findInCache(String requestJson, String producer) {
        Optional<GeneralProxyObsCacheDto> hit = cachingService.searchInCache(requestJson.hashCode())
                .filter(cached -> requestJson.equals(cached.getRequestText()));

        hit.ifPresent(cached -> cachingService.updateModifiedDateAndModifiedBy(producer, cached.getRequestHash()));
        return hit;
    }

    private void saveInCache(String requestJson, String responseJson, String producer) {
        cachingService.saveProxyCache(producer, requestJson.hashCode(), requestJson, responseJson);
    }

    public GeneralProxyObsDto fetchCheckEntity(String businessKey, String flowId) {
        return checkEntityRepository.findTopByBusinessKeyAndFlowIdOrderByCreatedDateDesc(businessKey, flowId)
                .map(checkEntity -> {
                    log.info("Fetching check entity with businessKey {} and flowId {}", businessKey, flowId);
                    return modelMapper.map(checkEntity, GeneralProxyObsDto.class);
                })
                .orElseGet(() -> {
                    log.info("Check entity with businessKey {} and flowId {} not found", businessKey, flowId);
                    return new GeneralProxyObsDto();
                });
    }

    public GeneralProxyObsDto fetchCheckPerson(String businessKey, String flowId) {
        return checkPersonRepository.findTopByBusinessKeyAndFlowIdOrderByCreatedDateDesc(businessKey, flowId)
                .map(checkPersonEntity -> {
                    log.info("Fetching check person with businessKey {} and flowId {}", businessKey, flowId);
                    return modelMapper.map(checkPersonEntity, GeneralProxyObsDto.class);
                })
                .orElseGet(() -> {
                    log.info("Check person with businessKey {} and flowId {} not found", businessKey, flowId);
                    return new GeneralProxyObsDto();
                });
    }

    public GeneralProxyObsDto fetchCheckPersonInitial(String businessKey, String flowId) {
        return checkPersonInitialRepository.findTopByBusinessKeyAndFlowIdOrderByCreatedDateDesc(businessKey, flowId)
                .map(checkPersonInitialEntity -> {
                    log.info("Fetching check person initial with businessKey {} and flowId {}", businessKey, flowId);
                    return modelMapper.map(checkPersonInitialEntity, GeneralProxyObsDto.class);
                })
                .orElseGet(() -> {
                    log.info("Check person initial with businessKey {} and flowId {} not found", businessKey, flowId);
                    return new GeneralProxyObsDto();
                });
    }

    private void logSaved(String table, ProcessingContext processingContext) {
        log.info("Saved check for businessKey {} and flowId {} into {} table",
                processingContext.getBusinessKey(), processingContext.getFlowId(), table);
    }

    // păstrează structura din errorJson-ul tău actual, dacă e alta
    private String errorJson(String code, String message) {
        return gson.toJson(Map.of("errors", Map.of("error", List.of(Map.of("code", code, "message", message)))));
    }

    /** Avro nu acceptă null în properties: fără mesaj, trimitem măcar tipul excepției. */
    private static String messageOf(Exception e) {
        return Objects.requireNonNullElse(e.getMessage(), e.getClass().getSimpleName());
    }

    private static <T> List<T> nullSafe(List<T> items) {
        return Objects.requireNonNullElse(items, List.of());
    }

    /** Rezultatul unui check: răspunsul de salvat, erorile DOAR ale acestui apel și data check-ului. */
    private record CheckOutcome(String responseJson, List<Error> errors, LocalDate checkDate) {
        ErrorHandler errorHandler() {
            ErrorHandler errorHandler = new ErrorHandler();
            errorHandler.setErrors(new ArrayList<>(errors));
            return errorHandler;
        }
    }
}
