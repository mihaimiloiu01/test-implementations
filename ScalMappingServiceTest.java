package com.bnp.pf.transformation.scalproxy.service; // păstrează package-ul tău

// importurile existente: ModelMapper, CachingService, ScalRestExternalClient, ScalApiException, CheckEntityRepository,
// CheckPersonRepository, CheckPersonInitialRepository, CheckEntity, CheckPersonEntity, CheckPersonInitialEntity,
// EntityDto, PersonDto, PersonInitialDto, CheckPersonQueryDto, CheckPersonInitialQueryDto, CheckResultDto,
// GeneralProxyObsDto, GeneralProxyObsCacheDto, ProcessingContext, Organisation

import com.bnp.pf.transformation.commons.api.error.CollectError;
import com.bnp.pf.transformation.commons.api.error.ErrorHandler;
import com.bnp.pf.transformation.commons.api.message.input.Customer;
import com.bnp.pf.transformation.commons.api.message.input.Loan;
import com.google.gson.Gson;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.bnp.pf.transformation.commons.api.generalproxyobs.util.ProxyObsConstant.ERROR_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MappingServiceTest {

    private static final String BUSINESS_KEY = "BK-001";
    private static final String FLOW_ID = "FLOW-1";
    private static final String PRODUCER = "producer";
    private static final String EMPTY_RESPONSE = "SCAL External API call returned an error or empty response";

    /**
     * Mapper-ul e apelat și pentru DTO-urile de request, și pentru entitățile salvate.
     * Lenient: altfel strict stubs ar raporta apelurile de salvare nestubuite drept PotentialStubbingProblem.
     */
    @Mock(strictness = Mock.Strictness.LENIENT)
    private ModelMapper modelMapper;
    @Mock
    private CachingService cachingService;
    @Mock
    private ScalRestExternalClient scalRestExternalClient;
    @Mock
    private CheckPersonRepository checkPersonRepository;
    @Mock
    private CheckPersonInitialRepository checkPersonInitialRepository;
    @Mock
    private CheckEntityRepository checkEntityRepository;
    @Mock
    private CollectError collectError;
    @Spy
    private Gson gson = new Gson();

    @InjectMocks
    private MappingService mappingService;

    private ProcessingContext processingContext;
    private Loan loan;
    private int initialIndex;

    @BeforeEach
    void setUp() {
        processingContext = new ProcessingContext();
        processingContext.setBusinessKey(BUSINESS_KEY);
        processingContext.setFlowId(FLOW_ID);
        processingContext.setProducer(PRODUCER);
        initialIndex = processingContext.getIndex().get();
        loan = new Loan();
    }

    // =====================================================================================
    // Helpers
    // =====================================================================================

    /** same(): doi Organisation/Customer goi pot fi equals (Lombok), iar stub-urile s-ar suprascrie. */
    private EntityDto givenEntityFor(Organisation organisation) {
        EntityDto entity = new EntityDto();
        when(modelMapper.map(same(organisation), eq(EntityDto.class))).thenReturn(entity);
        return entity;
    }

    /** DTO-urile construite pentru salvare (în ordinea apelurilor), mapate apoi pe entitatea dată. */
    private <T> List<GeneralProxyObsDto> savedFor(Class<T> entityClass, int expectedSaves) {
        ArgumentCaptor<GeneralProxyObsDto> saved = ArgumentCaptor.forClass(GeneralProxyObsDto.class);
        verify(modelMapper, times(expectedSaves)).map(saved.capture(), eq(entityClass));
        return saved.getAllValues();
    }

    private String noErrorsJson() {
        ErrorHandler errorHandler = new ErrorHandler();
        errorHandler.setErrors(new ArrayList<>());
        return gson.toJson(errorHandler);
    }

    // =====================================================================================
    // Tests
    // =====================================================================================

    @Test
    void checkScalProcess_whenEntityCheckSucceeds_shouldCacheAndSaveResponseWithoutErrors() {
        Organisation organisation = new Organisation();
        loan.setCompanies(List.of(organisation));
        EntityDto entity = givenEntityFor(organisation);
        CheckResultDto response = new CheckResultDto();
        String requestJson = gson.toJson(entity);
        String responseJson = gson.toJson(response);
        when(scalRestExternalClient.checkEntity(same(entity))).thenReturn(response);

        mappingService.checkScalProcess(processingContext, loan);

        GeneralProxyObsDto saved = savedFor(CheckEntity.class, 1).get(0);
        verify(cachingService).saveProxyCache(PRODUCER, requestJson.hashCode(), requestJson, responseJson);
        verify(checkEntityRepository).save(any());
        verifyNoInteractions(collectError);
        assertThat(saved.getBusinessKey()).isEqualTo(BUSINESS_KEY);
        assertThat(saved.getFlowId()).isEqualTo(FLOW_ID);
        assertThat(saved.getResponseEntityJson()).isEqualTo(responseJson);
        assertThat(saved.getErrorHandlerJson()).isEqualTo(noErrorsJson());
    }

    @Test
    void checkScalProcess_whenPersonCheckFails_shouldSaveErrorOnlyOnThatCheckAndStillCheckOtherCustomers() {
        Customer withFirstName = new Customer();
        withFirstName.setFirstName("John");
        Customer withoutFirstName = new Customer();
        loan.setCustomers(List.of(withFirstName, withoutFirstName));
        PersonDto person = new PersonDto();
        PersonInitialDto personInitial = new PersonInitialDto();
        when(modelMapper.map(same(withFirstName), eq(PersonDto.class))).thenReturn(person);
        when(modelMapper.map(same(withoutFirstName), eq(PersonInitialDto.class))).thenReturn(personInitial);
        when(scalRestExternalClient.checkPerson(any(CheckPersonQueryDto.class)))
                .thenThrow(new ScalApiException(400, "Invalid person", null));
        when(scalRestExternalClient.checkPersonInitial(any(CheckPersonInitialQueryDto.class)))
                .thenReturn(new CheckResultDto());

        mappingService.checkScalProcess(processingContext, loan);

        ArgumentCaptor<CheckPersonQueryDto> personQuery = ArgumentCaptor.forClass(CheckPersonQueryDto.class);
        ArgumentCaptor<CheckPersonInitialQueryDto> initialQuery = ArgumentCaptor.forClass(CheckPersonInitialQueryDto.class);
        verify(scalRestExternalClient).checkPerson(personQuery.capture());
        verify(scalRestExternalClient).checkPersonInitial(initialQuery.capture());
        verify(collectError).addError(processingContext, "502", "Invalid person");
        verify(collectError).addErrorSignal(same(processingContext), any());
        verify(cachingService).saveProxyCache(eq(PRODUCER), anyInt(), anyString(), anyString()); // doar cel fără eroare
        assertThat(personQuery.getValue().getPerson1()).isSameAs(person);
        assertThat(initialQuery.getValue().getPerson1()).isSameAs(personInitial);
        assertThat(processingContext.getErrorMap()).containsOnly(entry(ERROR_MESSAGE + initialIndex, "Invalid person"));
        assertThat(savedFor(CheckPersonEntity.class, 1).get(0).getErrorHandlerJson()).contains("502", "Invalid person");
        assertThat(savedFor(CheckPersonInitialEntity.class, 1).get(0).getErrorHandlerJson()).isEqualTo(noErrorsJson());
    }

    @Test
    void checkScalProcess_whenScalReturnsNothingOrCallBreaks_shouldRegister502And555ErrorsAndNotCache() {
        Organisation emptyResponse = new Organisation();
        Organisation brokenCall = new Organisation();
        loan.setCompanies(List.of(emptyResponse, brokenCall));
        EntityDto emptyResponseEntity = givenEntityFor(emptyResponse);
        EntityDto brokenCallEntity = givenEntityFor(brokenCall);
        when(scalRestExternalClient.checkEntity(same(emptyResponseEntity))).thenReturn(null);
        when(scalRestExternalClient.checkEntity(same(brokenCallEntity))).thenThrow(new RuntimeException("Read timed out"));

        mappingService.checkScalProcess(processingContext, loan);

        List<GeneralProxyObsDto> saved = savedFor(CheckEntity.class, 2);
        verify(collectError).addError(processingContext, "502", EMPTY_RESPONSE);
        verify(collectError).addError(processingContext, "555", "Read timed out");
        verify(collectError, times(2)).addErrorSignal(same(processingContext), any());
        verify(cachingService, never()).saveProxyCache(any(), anyInt(), any(), any());
        assertThat(processingContext.getErrorMap()).containsOnly(
                entry(ERROR_MESSAGE + initialIndex, EMPTY_RESPONSE),
                entry(ERROR_MESSAGE + (initialIndex + 1), "Read timed out"));
        // fiecare check salvat are DOAR eroarea lui
        assertThat(saved.get(0).getErrorHandlerJson()).contains("502").doesNotContain("Read timed out");
        assertThat(saved.get(1).getErrorHandlerJson()).contains("555", "Read timed out").doesNotContain(EMPTY_RESPONSE);
    }

    @Test
    void checkScalProcess_whenResponseIsCached_shouldSaveCachedResponseWithoutCallingScal() {
        Organisation organisation = new Organisation();
        loan.setCompanies(List.of(organisation));
        EntityDto entity = givenEntityFor(organisation);
        String requestJson = gson.toJson(entity);
        LocalDate cachedDate = LocalDate.of(2026, 1, 10);
        GeneralProxyObsCacheDto cached = new GeneralProxyObsCacheDto();
        cached.setRequestHash(42);
        cached.setRequestText(requestJson);
        cached.setResponseText("{\"cached\":true}");
        cached.setCreatedDate(cachedDate);
        when(cachingService.searchInCache(requestJson.hashCode())).thenReturn(Optional.of(cached));

        mappingService.checkScalProcess(processingContext, loan);

        GeneralProxyObsDto saved = savedFor(CheckEntity.class, 1).get(0);
        verify(cachingService).updateModifiedDateAndModifiedBy(PRODUCER, 42);
        verifyNoInteractions(scalRestExternalClient, collectError);
        assertThat(saved.getResponseEntityJson()).isEqualTo("{\"cached\":true}");
        assertThat(saved.getErrorHandlerJson()).isEqualTo(noErrorsJson());
        assertThat(saved.getCreation().toLocalDate()).isEqualTo(cachedDate);
    }

    @Test
    void checkScalProcess_whenCachedRequestBelongsToAnotherClient_shouldIgnoreCacheAndCallScal() {
        Customer customer = new Customer();
        loan.setCustomers(List.of(customer));
        when(modelMapper.map(same(customer), eq(PersonInitialDto.class))).thenReturn(new PersonInitialDto());
        GeneralProxyObsCacheDto otherClient = new GeneralProxyObsCacheDto();
        otherClient.setRequestText("{\"another\":\"client\"}"); // același hash, alt request
        when(cachingService.searchInCache(anyInt())).thenReturn(Optional.of(otherClient));
        when(scalRestExternalClient.checkPersonInitial(any(CheckPersonInitialQueryDto.class)))
                .thenReturn(new CheckResultDto());

        mappingService.checkScalProcess(processingContext, loan);

        verify(cachingService, never()).updateModifiedDateAndModifiedBy(any(), anyInt());
        assertThat(mockingDetails(scalRestExternalClient).getInvocations()).hasSize(1);
    }

    @Test
    void checkScalProcess_whenLoanHasNoCompaniesOrCustomers_shouldDoNothing() {
        mappingService.checkScalProcess(processingContext, loan);

        verifyNoInteractions(scalRestExternalClient, cachingService, collectError);
        assertThat(mockingDetails(modelMapper).getInvocations()).isEmpty();
    }

    @Test
    void fetch_whenEntitiesExist_shouldReturnMappedDtos() {
        CheckEntity checkEntity = new CheckEntity();
        CheckPersonEntity checkPersonEntity = new CheckPersonEntity();
        CheckPersonInitialEntity checkPersonInitialEntity = new CheckPersonInitialEntity();
        GeneralProxyObsDto entityDto = new GeneralProxyObsDto();
        GeneralProxyObsDto personDto = new GeneralProxyObsDto();
        GeneralProxyObsDto personInitialDto = new GeneralProxyObsDto();
        when(checkEntityRepository.findTopByBusinessKeyAndFlowIdOrderByCreatedDateDesc(BUSINESS_KEY, FLOW_ID))
                .thenReturn(Optional.of(checkEntity));
        when(checkPersonRepository.findTopByBusinessKeyAndFlowIdOrderByCreatedDateDesc(BUSINESS_KEY, FLOW_ID))
                .thenReturn(Optional.of(checkPersonEntity));
        when(checkPersonInitialRepository.findTopByBusinessKeyAndFlowIdOrderByCreatedDateDesc(BUSINESS_KEY, FLOW_ID))
                .thenReturn(Optional.of(checkPersonInitialEntity));
        when(modelMapper.map(same(checkEntity), eq(GeneralProxyObsDto.class))).thenReturn(entityDto);
        when(modelMapper.map(same(checkPersonEntity), eq(GeneralProxyObsDto.class))).thenReturn(personDto);
        when(modelMapper.map(same(checkPersonInitialEntity), eq(GeneralProxyObsDto.class))).thenReturn(personInitialDto);

        assertThat(mappingService.fetchCheckEntity(BUSINESS_KEY, FLOW_ID)).isSameAs(entityDto);
        assertThat(mappingService.fetchCheckPerson(BUSINESS_KEY, FLOW_ID)).isSameAs(personDto);
        assertThat(mappingService.fetchCheckPersonInitial(BUSINESS_KEY, FLOW_ID)).isSameAs(personInitialDto);
    }

    @Test
    void fetch_whenEntitiesMissing_shouldReturnEmptyDtos() {
        // repository-urile mock întorc implicit Optional.empty()
        List<GeneralProxyObsDto> fetched = List.of(
                mappingService.fetchCheckEntity(BUSINESS_KEY, FLOW_ID),
                mappingService.fetchCheckPerson(BUSINESS_KEY, FLOW_ID),
                mappingService.fetchCheckPersonInitial(BUSINESS_KEY, FLOW_ID));

        assertThat(fetched).allSatisfy(dto -> assertThat(dto).usingRecursiveComparison().isEqualTo(new GeneralProxyObsDto()));
    }
}
