package com.santander.reportes.statementprofile.service.impl;

import java.io.File;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import com.santander.reportes.common.component.ApiRestTemplate;
import com.santander.reportes.common.component.AppContext;
import com.santander.reportes.common.model.DatosUsuario;
import com.santander.reportes.common.model.FileAndReportRelation;
import com.santander.reportes.common.model.GeneralDataReport;
import com.santander.reportes.common.model.ObtInfoUsuario;
import com.santander.reportes.common.service.CommonHelperService;
import com.santander.reportes.common.service.imp.ReportTranslationServiceIImpl;
import com.santander.reportes.downloadfiles.service.DownloadFilesServiceAsync;
import com.santander.reportes.downloadfiles.service.impl.CSVDocument;
import com.santander.reportes.downloadfiles.service.impl.ExcelDocument;
import com.santander.reportes.downloadfiles.service.impl.FileUtil;
import com.santander.reportes.downloadfiles.service.impl.PDFDocument;
import com.santander.reportes.searchaccounts.model.Account;
import com.santander.reportes.searchaccounts.model.SearchAccountsRequest;
import com.santander.reportes.searchaccounts.model.SearchAccountsResponse;
import com.santander.reportes.searchaccounts.service.SearchAccountsService;
import com.santander.reportes.statementprofile.model.DeleteStatementProfileRequest;
import com.santander.reportes.statementprofile.model.DeleteStatementProfileResponse;
import com.santander.reportes.statementprofile.model.GetStatementProfileRequest;
import com.santander.reportes.statementprofile.model.GetStatementProfileResponse;
import com.santander.reportes.statementprofile.model.SortReport;
import com.santander.reportes.statementprofile.model.SortReportMapper;
import com.santander.reportes.statementprofile.model.StatementDocumentData;
import com.santander.reportes.statementprofile.model.StatementGroup;
import com.santander.reportes.statementprofile.model.StatementGroupMapper;
import com.santander.reportes.statementprofile.model.StatementMovement;
import com.santander.reportes.statementprofile.model.StatementMovementMapper;
import com.santander.reportes.statementprofile.model.StatementProfileRequest;
import com.santander.reportes.statementprofile.model.StatementProfileResponse;
import com.santander.reportes.statementprofile.model.ValidIDReportRequest;
import com.santander.reportes.statementprofile.model.ValidIDReportResponse;
import com.santander.reportes.statementprofile.service.StatementProfileService;
import com.santander.serenity.devstack.jwt.model.JwtDetails;
import com.santander.serenity.devstack.jwt.validation.JwtAuthenticationToken;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class StatementProfileServiceImpl implements StatementProfileService {

	private static final String STR_KO = "KO";
	private static final String STR_OK = "OK";
	private static final String STR_TRS = "TRS";
	private static final String STR_STS = "STS";
	private static final String STR_STRE = "STRE";
	private static final String STR_N = "N";
	private static final String STR_DESC = "desc";
	private static final String STR_S = "S";
	private static final String STR_VACIO = "";
	private static final String STR_FORMAT_DATE = "yyyy-MM-dd";

	@Autowired
	private CommonHelperService commonHelperService;

	@Autowired
	private SearchAccountsService searchAccountsService;

	@Autowired
	private ReportTranslationServiceIImpl reportTranslationServiceIImpl;

	@Autowired
	private DownloadFilesServiceAsync downloadFilesServiceAsync;

	@Autowired
	private AppContext appContext;

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

	@Value("${schema_proc}")
	protected String schemaproc;

	@Value("${pathSaveFiles}")
	protected String pathSaveFiles;

	@Value("${urlReports}")
	protected String urlReports;

	@Autowired
	private ApiRestTemplate apiRestTemplate;

	@Override
	public StatementProfileResponse saveStatementProfileService(String cookie,
			StatementProfileRequest statementProfileRequest) {

		StatementProfileResponse statementProfileResponse = new StatementProfileResponse();

		// Obtenemos el usuario logado
		String uidLogado = STR_VACIO;
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (Objects.nonNull(SecurityContextHolder.getContext()) && auth instanceof JwtAuthenticationToken
				&& Objects.nonNull(auth.getDetails())) {

			JwtDetails santanderUD = (JwtDetails) auth.getDetails();
			uidLogado = santanderUD.getUid();
		}
		
		ObtInfoUsuario user = new ObtInfoUsuario();
		user = commonHelperService.obtInfoUsuario();
		String isDummy = user.getIsDummy();
		
		if(STR_N.equals(isDummy)) {
		// Datos comunes de los reportes
		String businessGroup = statementProfileRequest.getBusinessGroup().trim();
		String reportType = statementProfileRequest.getReportType().trim();
		String selectRelativeFrom = statementProfileRequest.getSelectRelativeFrom().trim();
		String selectRelativeTo = statementProfileRequest.getSelectRelativeTo().trim();
		String selectFrequency = statementProfileRequest.getSelectFrequency().trim();
		String tipDocument = statementProfileRequest.getTipDocument().trim();
		String reportId = statementProfileRequest.getReportId().trim();
		String reportDesc = statementProfileRequest.getReportDesc().trim();
		String dateDataRangeFrom = statementProfileRequest.getDateDataRangeFrom().trim();
		String dateDataRangeTo = statementProfileRequest.getDateDataRangeTo().trim();
		String dateFrequecy = statementProfileRequest.getDateFrequecy().trim();
		String startTime = statementProfileRequest.getStartTime().trim();
		String currency = " ";
		String idInt = statementProfileRequest.getIdInt().trim();
		String pendiente = "";

		// Validamos que el ID y la des cripcion son unicos en un alta
		int numReportId = 0;
		int numReporDesc = 0;
		if (idInt.length() == 0) {
			numReportId = commonHelperService.validReportId(reportId, uidLogado, idInt);
			numReporDesc = commonHelperService.validDescription(reportDesc, uidLogado, idInt);
		}

		if (numReportId == 0 && numReporDesc == 0) {
			if (!dateFrequecy.equals("") && !startTime.equals("")) {
				// convertir datos para frecuencia a GMT España
				Date fechaGMTEspana = commonHelperService.convertFechaUsuAEsp(dateFrequecy, startTime,
						appContext.getUserData().getFormatoZona());
				if (fechaGMTEspana != null) {
					startTime = new SimpleDateFormat("HH:mm").format(fechaGMTEspana);
					dateFrequecy = new SimpleDateFormat("yyyy-MM-dd").format(fechaGMTEspana).concat(" 00:00:00");
				}
			}

			if ("true".equals(statementProfileRequest.getIsDataRange())) {
				pendiente = "N";
			} else {
				pendiente = "S";
			}

			// Verificamos si es un alta o una modificacion
			if (idInt.length() > 0) {
				// Acutalizamos los datos comunes
				commonHelperService.updateCommonReportData(businessGroup, reportType, selectRelativeFrom,
						selectRelativeTo, selectFrequency, tipDocument, reportId, reportDesc, dateDataRangeFrom,
						dateDataRangeTo, dateFrequecy, startTime, currency, uidLogado, idInt, pendiente);

				// Eliminamos las cuentas previas
				commonHelperService.deleteAccountsReport(idInt);

				// Eliminamos las ordenaciones previas
				deleteSortReport(idInt);

			} else {
				idInt = commonHelperService.insertCommonReportData(businessGroup, reportType, selectRelativeFrom,
						selectRelativeTo, selectFrequency, tipDocument, reportId, reportDesc, dateDataRangeFrom,
						dateDataRangeTo, dateFrequecy, startTime, currency, uidLogado, pendiente);
			}

			// Cuentas seleccionadas del reporte
			commonHelperService.insertAccountsReport(statementProfileRequest.getBusinessGroup(),
					statementProfileRequest.getCuentas(), idInt);

			// Statement ordenacion
			if (statementProfileRequest.getSelectStatementOne() != null
					&& statementProfileRequest.getSelectStatementOne().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioStatementOne().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_STS, statementProfileRequest.getSelectStatementOne(), isAsc, idInt, 1);
			}
			if (statementProfileRequest.getSelectStatementTwo() != null
					&& statementProfileRequest.getSelectStatementTwo().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioStatementTwo().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_STS, statementProfileRequest.getSelectStatementTwo(), isAsc, idInt, 2);
			}
			if (statementProfileRequest.getSelectStatementThree() != null
					&& statementProfileRequest.getSelectStatementThree().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioStatementThree().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_STS, statementProfileRequest.getSelectStatementThree(), isAsc, idInt, 3);
			}
			if (statementProfileRequest.getSelectStatementFour() != null
					&& statementProfileRequest.getSelectStatementFour().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioStatementFour().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_STS, statementProfileRequest.getSelectStatementFour(), isAsc, idInt, 4);
			}

			// Transaction ordenacion
			if (statementProfileRequest.getSelectTransactionOne() != null
					&& statementProfileRequest.getSelectTransactionOne().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioTransactionOne().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_TRS, statementProfileRequest.getSelectTransactionOne(), isAsc, idInt, 1);
			}
			if (statementProfileRequest.getSelectTransactionTwo() != null
					&& statementProfileRequest.getSelectTransactionTwo().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioTransactionTwo().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_TRS, statementProfileRequest.getSelectTransactionTwo(), isAsc, idInt, 2);
			}
			if (statementProfileRequest.getSelectTransactionThree() != null
					&& statementProfileRequest.getSelectTransactionThree().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioTransactionThree().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_TRS, statementProfileRequest.getSelectTransactionThree(), isAsc, idInt,
						3);
			}
			if (statementProfileRequest.getSelectTransactionFour() != null
					&& statementProfileRequest.getSelectTransactionFour().length() > 0) {
				String isAsc = STR_S;
				if (statementProfileRequest.getRadioTransactionFour().trim().equals(STR_DESC)) {
					isAsc = STR_N;
				}
				insertSortReport(STR_STRE, STR_TRS, statementProfileRequest.getSelectTransactionFour(), isAsc, idInt,
						4);
			}

			if ("true".equals(statementProfileRequest.getIsDataRange())) {
				if (idInt.length() > 0) {
					downloadFilesServiceAsync.generateStatementReportDataAsinc(idInt, uidLogado);
				} else {
					String idIntCurrent = commonHelperService.getCurrentValue();
					downloadFilesServiceAsync.generateStatementReportDataAsinc(idIntCurrent, uidLogado);
				}
			}

			statementProfileResponse.setStatus(STR_OK);
			statementProfileResponse.setMessage("Operación correcta");
		} else {
			statementProfileResponse.setStatus(STR_KO);
			if (numReportId > 0) {
				statementProfileResponse.setMessage("Id duplicado");
			} else {
				statementProfileResponse.setMessage("Descripcion duplicada");
			}

		}
		} else {
			statementProfileResponse.setStatus(STR_OK);
			statementProfileResponse.setMessage("Operación correcta");
		}

		return statementProfileResponse;
	}

	@Override
	public GetStatementProfileResponse getStatementProfileService(String cookie,
			GetStatementProfileRequest getStatementProfileRequest) {
		GetStatementProfileResponse getStatementProfileResponse = new GetStatementProfileResponse();

		// Obtenemos el usuario logado
		String uidLogado = STR_VACIO;
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (Objects.nonNull(SecurityContextHolder.getContext()) && auth instanceof JwtAuthenticationToken
				&& Objects.nonNull(auth.getDetails())) {

			JwtDetails santanderUD = (JwtDetails) auth.getDetails();
			uidLogado = santanderUD.getUid();
		}
		getStatementProfileResponse.setFormatoZona(appContext.getUserData().getFormatoZona());
		// Recuperamos los datos generales
		List<GeneralDataReport> datos = commonHelperService.getGeneraDataReport(getStatementProfileRequest.getIndInt(),
				uidLogado);
		if (datos.size() > 0) {
			getStatementProfileResponse.setReportId(datos.get(0).getReportId().trim());
			getStatementProfileResponse.setReportDesc(datos.get(0).getReportDesc().trim());
			getStatementProfileResponse.setDateDataRangeFrom(datos.get(0).getDateDataRangeFrom().trim());
			getStatementProfileResponse.setDateDataRangeTo(datos.get(0).getDateDataRangeTo().trim());
			getStatementProfileResponse.setSelectRelativeFrom(datos.get(0).getSelectRelativeFrom().trim());
			getStatementProfileResponse.setSelectRelativeTo(datos.get(0).getSelectRelativeTo().trim());
			getStatementProfileResponse.setSelectFrequency(datos.get(0).getSelectFrequency().trim());
			getStatementProfileResponse.setDateFrequecy(datos.get(0).getDateFrequecy().trim());
			getStatementProfileResponse.setStartTime(datos.get(0).getStartTime().trim());
			getStatementProfileResponse.setTipDocument(datos.get(0).getTipDocument().trim());
			getStatementProfileResponse.setBusinessGroup(datos.get(0).getBusinessGroup().trim());
			getStatementProfileResponse.setReportType(datos.get(0).getReportType().trim());

			if (!datos.get(0).getDateFrequecy().trim().equals("") && !datos.get(0).getStartTime().trim().equals("")
					&& !datos.get(0).getDateFrequecy().trim().equals("0001-01-01 00:00:00")
					&& !datos.get(0).getStartTime().equals("datos.get(0).getStartTime()")) {
				// convertir datos para frecuencia a GMT Usuario
				Date fechaGMTUsuario = commonHelperService.convertFechaEspAGMTUsuario(
						datos.get(0).getDateFrequecy().trim(), datos.get(0).getStartTime().trim(),
						appContext.getUserData().getFormatoZona());
				if (fechaGMTUsuario != null) {
					String startTime = "0001-12-31 ".concat(new SimpleDateFormat("HH:mm:ss").format(fechaGMTUsuario));
					String dateFrecuency = new SimpleDateFormat("yyyy-MM-dd").format(fechaGMTUsuario)
							.concat(" 00:00:00");
					getStatementProfileResponse.setStartTime(startTime);
					getStatementProfileResponse.setDateFrequecy(dateFrecuency);
				}
			}

			// Determinamos si es data range o relative data range
			if ("0001-01-01 00:00:00".equals(getStatementProfileResponse.getDateDataRangeFrom())
					&& "0001-01-01 00:00:00".equals(getStatementProfileResponse.getDateDataRangeTo())) {
				getStatementProfileResponse.setIsDataRange("false");
				getStatementProfileResponse.setIsRelativeDataRange("true");
			} else {
				getStatementProfileResponse.setIsDataRange("true");
				getStatementProfileResponse.setIsRelativeDataRange("false");
			}

		}

		// Recuperamos la lista de cuentas seleccionadas
		List<Integer> listCodAccount = commonHelperService
				.getListAccountSelected(getStatementProfileRequest.getIndInt());
		getStatementProfileResponse.setCuentasSelected(searchAccountsService.getAccount(listCodAccount));

		// Recuperamos los alias de las cuentas seleccionada
		for (Account ac : getStatementProfileResponse.getCuentasSelected()) {
			ac.setAliasAccount(searchAccountsService.getAliasAccount(ac.getCodAccount(), uidLogado));
		}

		SearchAccountsRequest searchAccountsRequest = new SearchAccountsRequest();
		SearchAccountsResponse cuentasPerfiladas = searchAccountsService.getListAccountImpl(cookie,
				searchAccountsRequest);
		getStatementProfileResponse.setCuentas(cuentasPerfiladas.getAccountList());
		getStatementProfileResponse.setBankEntityList(cuentasPerfiladas.getBankEntityList());
		getStatementProfileResponse.setCurrencyList(cuentasPerfiladas.getCurrencyList());
		getStatementProfileResponse.setCorporateList(cuentasPerfiladas.getCorporateList());
		getStatementProfileResponse.setCountryList(cuentasPerfiladas.getCountryList());

		// Recupeamos los datos de ordenacion Statement
		List<SortReport> sortData = getSortReport(getStatementProfileRequest.getIndInt(), "STRE");
		for (SortReport sort : sortData) {
			if (sort.getTypeOrder().equals("STS")) {
				if (sort.getNumOrden() == 1) {
					getStatementProfileResponse.setSelectStatementOne(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioStatementOne("asc");
					} else {
						getStatementProfileResponse.setRadioStatementOne("desc");
					}
				} else if (sort.getNumOrden() == 2) {
					getStatementProfileResponse.setSelectStatementTwo(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioStatementTwo("asc");
					} else {
						getStatementProfileResponse.setRadioStatementTwo("desc");
					}
				} else if (sort.getNumOrden() == 3) {
					getStatementProfileResponse.setSelectStatementThree(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioStatementThree("asc");
					} else {
						getStatementProfileResponse.setRadioStatementThree("desc");
					}
				} else if (sort.getNumOrden() == 4) {
					getStatementProfileResponse.setSelectStatementFour(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioStatementFour("asc");
					} else {
						getStatementProfileResponse.setRadioStatementFour("desc");
					}
				}

			} else {

				if (sort.getNumOrden() == 1) {
					getStatementProfileResponse.setSelectTransactionOne(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioTransactionOne("asc");
					} else {
						getStatementProfileResponse.setRadioTransactionOne("desc");
					}
				} else if (sort.getNumOrden() == 2) {
					getStatementProfileResponse.setSelectTransactionTwo(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioTransactionTwo("asc");
					} else {
						getStatementProfileResponse.setRadioTransactionTwo("desc");
					}
				} else if (sort.getNumOrden() == 3) {
					getStatementProfileResponse.setSelectTransactionThree(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioTransactionThree("asc");
					} else {
						getStatementProfileResponse.setRadioTransactionThree("desc");
					}
				} else if (sort.getNumOrden() == 4) {
					getStatementProfileResponse.setSelectTransactionFour(sort.getTyporOrderData());
					if (("S").equals(sort.getIsAsc())) {
						getStatementProfileResponse.setRadioTransactionFour("asc");
					} else {
						getStatementProfileResponse.setRadioTransactionFour("desc");
					}
				}

			}
		}

		getStatementProfileResponse.setStatus(STR_OK);
		getStatementProfileResponse.setMessage("Operación correcta");

		return getStatementProfileResponse;
	}

	@Override
	public DeleteStatementProfileResponse deleteStatementProfileService(String cookie,
			DeleteStatementProfileRequest deleteStatementProfileRequest) {

		DeleteStatementProfileResponse deleteStatementProfileResponse = new DeleteStatementProfileResponse();

		// Obtenemos el usuario logado
		String uidLogado = STR_VACIO;
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (Objects.nonNull(SecurityContextHolder.getContext()) && auth instanceof JwtAuthenticationToken
				&& Objects.nonNull(auth.getDetails())) {

			JwtDetails santanderUD = (JwtDetails) auth.getDetails();
			uidLogado = santanderUD.getUid();
		}

		commonHelperService.deleteReport(deleteStatementProfileRequest.getIndInt().trim(), uidLogado);

		deleteStatementProfileResponse.setStatus(STR_OK);
		deleteStatementProfileResponse.setMessage("Operación correcta");

		return deleteStatementProfileResponse;
	}

	@Override
	public ValidIDReportResponse validIDReportService(String cookie, ValidIDReportRequest validIDReportRequest) {
		ValidIDReportResponse validIDReportResponse = new ValidIDReportResponse();

		// Obtenemos el usuario logado
		String uidLogado = STR_VACIO;
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		if (Objects.nonNull(SecurityContextHolder.getContext()) && auth instanceof JwtAuthenticationToken
				&& Objects.nonNull(auth.getDetails())) {

			JwtDetails santanderUD = (JwtDetails) auth.getDetails();
			uidLogado = santanderUD.getUid();
		}

		// Validamos si existe el id del reporte
		int countId = commonHelperService.validReportId(validIDReportRequest.getId().trim(), uidLogado,
				validIDReportRequest.getIdInt());

		// Validamos si existe la descripcion del reporte
		int countDesc = commonHelperService.validDescription(validIDReportRequest.getDesc().trim(), uidLogado,
				validIDReportRequest.getIdInt());

		if (countId == 0 && countDesc == 0) {
			validIDReportResponse.setStatus(STR_OK);
			validIDReportResponse.setMessage("Operación correcta");
		} else if (countId > 0) {
			validIDReportResponse.setStatus(STR_KO);
			validIDReportResponse.setMessage("El ID no es único");
		} else {
			validIDReportResponse.setStatus(STR_KO);
			validIDReportResponse.setMessage("La descripción no es única");
		}

		return validIDReportResponse;
	}

	/*** Metodos privados ***/
	public void insertSortReport(String cdtiprep, String cdtpssor, String cdtipsor, String ordenasc, String idInt,
			int orden) {

		String sql = "";
		if (idInt.length() > 0) {
			sql = "INSERT INTO  " + schemaproc
					+ ".SGP_R_PFLRP_SRT(T2182_CDPFLREP, T2182_CDTIPREP, T2182_CDTPSSOR, T2182_CDTIPSOR, T2182_ORDENASC, T2182_SRTORDEN) VALUES(?, ?, ?, ?, ?, ?)";

			Object[] params = new Object[] { idInt, cdtiprep, cdtpssor, cdtipsor, ordenasc, orden };
			jdbcTemplate.update(sql, params);
		} else {
			String currIdInt = commonHelperService.getCurrentValue();
			sql = "INSERT INTO  " + schemaproc
					+ ".SGP_R_PFLRP_SRT(T2182_CDPFLREP, T2182_CDTIPREP, T2182_CDTPSSOR, T2182_CDTIPSOR, T2182_ORDENASC, T2182_SRTORDEN) VALUES("
					+ currIdInt + ", ?, ?, ?, ?, ?)";

			Object[] params = new Object[] { cdtiprep, cdtpssor, cdtipsor, ordenasc, orden };
			jdbcTemplate.update(sql, params);
		}

	}

	public List<SortReport> getSortReport(String idInt, String tipoReport) {
		String sql = "SELECT T2182_CDTIPREP, T2182_CDTPSSOR, T2182_CDTIPSOR, T2182_ORDENASC, T2182_SRTORDEN FROM "
				+ schemaproc + ".SGP_R_PFLRP_SRT WHERE T2182_CDPFLREP = ? AND T2182_CDTIPREP = ?";

		Object[] params = new Object[] { idInt, tipoReport };

		return jdbcTemplate.query(sql, params, new SortReportMapper());
	}

	public void deleteSortReport(String idInt) {
		String sql = "DELETE FROM " + schemaproc + ".SGP_R_PFLRP_SRT WHERE T2182_CDPFLREP = ?";

		Object[] params = new Object[] { idInt };

		jdbcTemplate.update(sql, params);
	}

	private void insertarFichero(File fichero, String idInt) {

		List<FileAndReportRelation> listaRelaciones = commonHelperService.getFileAndReportRelation(idInt);
		if (listaRelaciones.isEmpty()) {
			// Damos de alta el fichero
			commonHelperService.insertFileReport(pathSaveFiles, fichero);

			// Generamos la nueva relacion
			commonHelperService.insertFileAndReportRelation(idInt);
		} else {
			FileAndReportRelation relacion = listaRelaciones.get(0);
			// actualizamos el fichero
			commonHelperService.updateFileReport(fichero, relacion.getIdFich());
			// actualizamos la fecha de la relacion
			commonHelperService.updateFileAndReportRelationDate(relacion.getIdFich(), relacion.getIdInt());
		}

	}

	public void generateStatementReportData(String idInt, String uid) {
System.out.println();
		Date date = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat(STR_FORMAT_DATE);
		String reportGeneration = formatter.format(date);

		StatementDocumentData datosDocument = new StatementDocumentData();
		datosDocument.setReportGenerationDate(reportGeneration);
		datosDocument.setUid(uid);

		if (idInt.length() == 0) {
			idInt = commonHelperService.getCurrentValue();
		}

		// Recuperamos los datos comunes del Reporte
		List<GeneralDataReport> comunData = commonHelperService.getGeneraDataReport(idInt, "");
		datosDocument.setReportID(comunData.get(0).getReportId().trim());
		datosDocument.setReportDesc(comunData.get(0).getReportDesc().trim());

		// Recuperamos el rango de fechas a filtrar
		String fechIni = "";
		String fechEnd = "";
		if (!"0001-01-01 00:00:00".equals(comunData.get(0).getDateDataRangeFrom())
				&& !"0001-01-01 00:00:00".equals(comunData.get(0).getDateDataRangeTo())) {
			// Si es Data Range
			fechIni = comunData.get(0).getDateDataRangeFrom().trim();
			fechEnd = comunData.get(0).getDateDataRangeTo().trim();
			fechEnd = fechEnd.substring(0, 10).concat(" 23:59:59");

		} else {
			// Si es relative Data Range

			fechIni = commonHelperService.getDate(comunData.get(0).getSelectRelativeFrom());
			fechEnd = commonHelperService.getDate(comunData.get(0).getSelectRelativeTo());

			fechEnd = fechEnd.substring(0, fechEnd.indexOf(" ")).concat(" 23:59:59");

		}

		// Recuperamos las cuentas seleccionadas para el reporte
		List<Integer> listCodAccount = commonHelperService.getListAccountSelected(idInt);

		// Recuperamos las ordenaciones
		List<SortReport> sortData = getSortReport(idInt, "STRE");

		List<StatementGroup> listGroups = new ArrayList<>();

		// Recuperamos los datos por cada codigo de cuenta
		listGroups = selectStatementGroup(listCodAccount, sortData, fechIni, fechEnd);

		datosDocument.setGroup(listGroups);
		datosDocument.setPath(pathSaveFiles);

		DatosUsuario datosUser = commonHelperService.obtenerDatosUsuario(uid);

		String datosCabTraduc[] = reportTranslationServiceIImpl.getTranslation("statementReport",
				datosUser.getIdioma().trim().toUpperCase());

		if (listGroups.size() > 0) {
			if ("ALL".equals(comunData.get(0).getTipDocument().trim())) {
				File ficheroExcel = ExcelDocument.getStatemetReportExcel(datosCabTraduc, datosDocument, datosUser);
				File ficheroCSV = CSVDocument.getStatemetReportCSV(datosCabTraduc, datosDocument, datosUser);
				File ficheroPDF = PDFDocument.getStatemetReportPDF(datosCabTraduc, datosDocument, datosUser);

				listGroups = new ArrayList<>();
				listGroups = null;

				List<File> listFiles = new ArrayList<>();
				listFiles.add(ficheroCSV);
				listFiles.add(ficheroExcel);
				listFiles.add(ficheroPDF);
				File zip = FileUtil.makeZipFile(listFiles, comunData.get(0).getReportId().trim(), pathSaveFiles);

				ficheroExcel.delete();
				ficheroCSV.delete();
				ficheroPDF.delete();

				insertarFichero(zip, idInt);

				zip.delete();
			} else if ("CSV".equals(comunData.get(0).getTipDocument().trim())) {
				File ficheroCSV = CSVDocument.getStatemetReportCSV(datosCabTraduc, datosDocument, datosUser);

				listGroups = new ArrayList<>();
				listGroups = null;

				List<File> listFiles = new ArrayList<>();
				listFiles.add(ficheroCSV);
				File zip = FileUtil.makeZipFile(listFiles, comunData.get(0).getReportId().trim(), pathSaveFiles);

				insertarFichero(zip, idInt);
				zip.delete();
				ficheroCSV.delete();
			} else if ("XLS".equals(comunData.get(0).getTipDocument().trim())) {
				File ficheroExcel = ExcelDocument.getStatemetReportExcel(datosCabTraduc, datosDocument, datosUser);

				listGroups = new ArrayList<>();
				listGroups = null;

				List<File> listFiles = new ArrayList<>();
				listFiles.add(ficheroExcel);
				File zip = FileUtil.makeZipFile(listFiles, comunData.get(0).getReportId().trim(), pathSaveFiles);

				insertarFichero(zip, idInt);
				zip.delete();
				ficheroExcel.delete();
			} else if ("PDF".equals(comunData.get(0).getTipDocument().trim())) {
				File ficheroPDF = PDFDocument.getStatemetReportPDF(datosCabTraduc, datosDocument, datosUser);

				listGroups = new ArrayList<>();
				listGroups = null;

				List<File> listFiles = new ArrayList<>();
				listFiles.add(ficheroPDF);
				File zip = FileUtil.makeZipFile(listFiles, comunData.get(0).getReportId().trim(), pathSaveFiles);

				insertarFichero(zip, idInt);
				zip.delete();
				ficheroPDF.delete();
			}
		}
		datosDocument = new StatementDocumentData();
		datosDocument = null;

		closeConnection();

		commonHelperService.inicializeClassMovement();
	}

	private void closeConnection() {
		try {
			if (jdbcTemplate != null) {
				jdbcTemplate.getDataSource().getConnection().close();
			}
			if (namedParameterJdbcTemplate != null) {
				namedParameterJdbcTemplate.getJdbcTemplate().getDataSource().getConnection().close();
			}
		} catch (SQLException e) {
			log.debug("Error in mehod closeConnection", e);
		}

	}

	public List<StatementGroup> selectStatementGroup(List<Integer> acuenCods, List<SortReport> sortData, String fechIni,
			String fechEnd) {

		String sql = "SELECT H3270_ACUENCOT, H3270_FECCONTA, N2912_NENTEMIS, H1162_CODCTAEX,  H1162_CODMONSWI, H3270_IMPINIOR, H3270_IMPOSAL \r\n"
				+ "FROM " + schemaproc + ".SGP_SALDOS, " + schemaproc + ".SGP_CUENTA, " + schemaproc
				+ ".SGP_ENTBANCARIA \r\n" + "WHERE H3270_ACUENCOT = H1162_ACUENCOT \r\n"
				+ "AND H1162_CENTBSGP = N2912_CENTBSGP \r\n" + "AND H3270_ACUENCOT IN (:acuenCods) \r\n"
				+ "AND H3270_FECCONTA >= TO_TIMESTAMP(:fechIni, 'YYYY-MM-DD HH24:MI:SS')  \r\n"
				+ "AND H3270_FECCONTA <= TO_TIMESTAMP(:fechEnd, 'YYYY-MM-DD HH24:MI:SS') \r\n"
				+ "GROUP BY H3270_ACUENCOT, H3270_FECCONTA, N2912_NENTEMIS, H1162_CODCTAEX, H1162_CODMONSWI, H3270_IMPINIOR, H3270_IMPOSAL";

		// Verificamos las ordenaciones
		String ordenOne = "";
		String ordenTwo = "";
		String ordenThree = "";
		String ordenFour = "";
		int countOrder = 0;

		for (SortReport sort : sortData) {
			String order = "";
			if ("STS".equals(sort.getTypeOrder())) {

				if ("ACC".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H1162_CODCTAEX ASC";
					} else {
						order = "H1162_CODCTAEX DESC";
					}
				} else if ("CCY".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H1162_CODMONSWI ASC";
					} else {
						order = "H1162_CODMONSWI DESC";
					}
				} else if ("ACH".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "N2912_NENTEMIS ASC";
					} else {
						order = "N2912_NENTEMIS DESC";
					}
				} else if ("BDT".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H3270_FECCONTA ASC";
					} else {
						order = "H3270_FECCONTA DESC";
					}
				}

				if (sort.getNumOrden() == 1) {
					ordenOne = order;
					countOrder++;
				} else if (sort.getNumOrden() == 2) {
					ordenTwo = order;
					countOrder++;
				} else if (sort.getNumOrden() == 3) {
					ordenThree = order;
					countOrder++;
				} else if (sort.getNumOrden() == 4) {
					ordenFour = order;
					countOrder++;
				}

			}

		}

		boolean hayOtros = false;
		if (countOrder > 0) {
			String orderBy = "ORDER BY ";
			if (ordenOne.length() > 0) {
				orderBy = orderBy.concat(ordenOne);
				hayOtros = true;
			}

			if (ordenTwo.length() > 0) {
				if (hayOtros) {
					orderBy = orderBy.concat(", ").concat(ordenTwo);
				} else {
					orderBy = orderBy.concat(ordenTwo);
					hayOtros = true;
				}
			}

			if (ordenThree.length() > 0) {
				if (hayOtros) {
					orderBy = orderBy.concat(", ").concat(ordenThree);
				} else {
					orderBy = orderBy.concat(ordenThree);
					hayOtros = true;
				}
			}

			if (ordenFour.length() > 0) {
				if (hayOtros) {
					orderBy = orderBy.concat(", ").concat(ordenFour);
				} else {
					orderBy = orderBy.concat(ordenFour);
					hayOtros = true;
				}
			}

			if (orderBy.length() > 0) {
				sql = sql.concat(" ").concat(orderBy);
			}
		}

		MapSqlParameterSource params = new MapSqlParameterSource();
		params.addValue("acuenCods", acuenCods);
		params.addValue("fechIni", fechIni);
		params.addValue("fechEnd", fechEnd);
		List<StatementGroup> grupos = namedParameterJdbcTemplate.query(sql, params, new StatementGroupMapper());

		if (grupos.size() > 0) {
			for (StatementGroup g : grupos) {
				List<StatementMovement> movimientos = selectStatementMovement(Integer.parseInt(g.getAcuenCod()),
						g.getStatementDate(), g.getAccountHolder(), g.getAccount(), g.getCurrency(),
						g.getOpeningBalance(), g.getClosingBalance(), sortData);
				g.setList(movimientos);
			}

		}
		return grupos;

	}

	public List<StatementMovement> selectStatementMovement(int acuencod, String fechoper, String nentemis,
			String codctaex, String codmonswi, BigDecimal impinior, BigDecimal imposal, List<SortReport> sortData) {

		String sql = "SELECT H3272_TIPOSW, H3272_IMP_MTOS, H3272_REFE, H3272_REFBAN, H3272_FEC_VAL, H3272_MOVDESC "
				+ "FROM " + schemaproc + ".SGP_MOVIMIENTOS " + "WHERE H3272_ACUENCOT = ? "
				+ "AND H3272_FECAOPER = TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS.FF') ";

		// Verificamos las ordenaciones
		String ordenOne = "";
		String ordenTwo = "";
		String ordenThree = "";
		String ordenFour = "";
		int countOrder = 0;

		for (SortReport sort : sortData) {
			String order = "";
			if ("TRS".equals(sort.getTypeOrder())) {

				if ("CRF".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H3272_REFE ASC";
					} else {
						order = "H3272_REFE DESC";
					}
				} else if ("VDT".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H3272_FEC_VAL ASC";
					} else {
						order = "H3272_FEC_VAL DESC";
					}
				} else if ("TYP".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H3272_TIPOSW ASC";
					} else {
						order = "H3272_TIPOSW DESC";
					}
				} else if ("AMN".equals(sort.getTyporOrderData())) {
					if ("S".equals(sort.getIsAsc())) {
						order = "H3272_IMP_MTOS ASC";
					} else {
						order = "H3272_IMP_MTOS DESC";
					}
				}

				if (sort.getNumOrden() == 1) {
					ordenOne = order;
					countOrder++;
				} else if (sort.getNumOrden() == 2) {
					ordenTwo = order;
					countOrder++;
				} else if (sort.getNumOrden() == 3) {
					ordenThree = order;
					countOrder++;
				} else if (sort.getNumOrden() == 4) {
					ordenFour = order;
					countOrder++;
				}

			}

		}

		boolean hayOtros = false;
		if (countOrder > 0) {
			String orderBy = "ORDER BY ";
			if (ordenOne.length() > 0) {
				orderBy = orderBy.concat(ordenOne);
				hayOtros = true;
			}

			if (ordenTwo.length() > 0) {
				if (hayOtros) {
					orderBy = orderBy.concat(", ").concat(ordenTwo);
				} else {
					orderBy = orderBy.concat(ordenTwo);
					hayOtros = true;
				}
			}

			if (ordenThree.length() > 0) {
				if (hayOtros) {
					orderBy = orderBy.concat(", ").concat(ordenThree);
				} else {
					orderBy = orderBy.concat(ordenThree);
					hayOtros = true;
				}
			}

			if (ordenFour.length() > 0) {
				if (hayOtros) {
					orderBy = orderBy.concat(", ").concat(ordenFour);
				} else {
					orderBy = orderBy.concat(ordenFour);
					hayOtros = true;
				}
			}

			if (orderBy.length() > 0) {
				sql = sql.concat(" ").concat(orderBy);
			}
		}
		Object[] params = new Object[] { acuencod, fechoper };
		return jdbcTemplate.query(sql, params, new StatementMovementMapper());

	}

}
