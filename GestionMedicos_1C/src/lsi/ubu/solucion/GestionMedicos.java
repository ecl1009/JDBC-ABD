package lsi.ubu.solucion;

import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.enunciado.GestionMedicosException;
import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;
import lsi.ubu.util.exceptions.SGBDError;
import lsi.ubu.util.exceptions.oracle.OracleSGBDErrorUtil;

import static org.junit.Assert.assertEquals;

/**
 * GestionMedicos: Implementa la gestion de medicos segun el PDF de la carpeta
 * enunciado
 * 
 * @author <a href="mailto:ecl1009@alu.ubu.es">Eduardo Manuel Cabeza Lopez</a>
 * @version 1.0
 * @since 1.0
 */
public class GestionMedicos {

	private static Logger logger = LoggerFactory.getLogger(GestionMedicos.class);

	private static final String script_path = "sql/";

	/**
	 * Main: Método main que ejecuta el método tests()
	 * 
	 * @param args
	 * @throws SQLException
	 */
	public static void main(String[] args) throws SQLException {
		tests();
		System.out.println("FIN.............");
	}

	/**
	 * reservar_consulta: Método que implementa la transacción para reservar una
	 * consulta médica en la fecha especificada. Recibe el NIF del cliente y del
	 * médico así como la fecha para la que se quiere reservar consulta. Si alguno
	 * de los datos no se encuentra en la base de datos o la fecha ya se encuentra
	 * ocupada lanza una GestionMedicosException.
	 * 
	 * @param m_NIF_cliente    NIF del cliente
	 * @param m_NIF_medico     NIF del médico
	 * @param m_Fecha_Consulta Fecha en la que se quiere reservar
	 * @throws SQLException
	 */
	public static void reservar_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta)
			throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement pst_sel_cliente = null;
		PreparedStatement pst_sel_medico = null;
		PreparedStatement pst_sel_consulta = null;
		PreparedStatement pst_sel_anulacion = null;
		PreparedStatement pst_ins_consulta = null;
		PreparedStatement pst_upd_medico = null;
		ResultSet rs_sel_cliente = null;
		ResultSet rs_sel_medico = null;
		ResultSet rs_sel_consulta = null;
		ResultSet rs_sel_anulacion = null;

		try {
			con = pool.getConnection();

			pst_sel_medico = con.prepareStatement("select id_medico from medico where NIF=?");
			pst_sel_medico.setString(1, m_NIF_medico);
			rs_sel_medico = pst_sel_medico.executeQuery();
			if (!rs_sel_medico.next()) {
				throw new GestionMedicosException(2);
			}

			int idMedico = rs_sel_medico.getInt(1);
			java.sql.Date m_Fecha_sql = new java.sql.Date(m_Fecha_Consulta.getTime());

			pst_ins_consulta = con.prepareStatement("insert into consulta " + "values (seq_consulta.nextval, ?, ?, ?)");
			pst_ins_consulta.setDate(1, m_Fecha_sql);
			pst_ins_consulta.setInt(2, idMedico);
			pst_ins_consulta.setString(3, m_NIF_cliente);
			pst_ins_consulta.executeUpdate();
			pst_upd_medico = con.prepareStatement("update medico set consultas = consultas + ? where id_medico = ?"
					+ " and (select count(*) from consulta join anulacion on consulta.id_consulta=anulacion.id_consulta"
					+ " where fecha_consulta=? and consulta.id_medico = ?)+1"
					+ "=(select count(*) from consulta where fecha_consulta=? and id_medico=?)");
			pst_upd_medico.setInt(1, 1);
			pst_upd_medico.setInt(2, idMedico);
			pst_upd_medico.setDate(3, m_Fecha_sql);
			pst_upd_medico.setInt(4, idMedico);
			pst_upd_medico.setDate(5, m_Fecha_sql);
			pst_upd_medico.setInt(6, idMedico);

			int numUpd = pst_upd_medico.executeUpdate();
			if (numUpd == 0) {
				throw new GestionMedicosException(3);
			}

			con.commit();

		} catch (SQLException e) {

			con.rollback();

			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException) e;
			}
			if (new OracleSGBDErrorUtil().checkExceptionToCode(e, SGBDError.FK_VIOLATED)) {
				throw new GestionMedicosException(1);
			}

			logger.error(e.getMessage());
			throw e;

		} finally {
			/* Se liberan todos los recursos que sean necesarios */
			if (rs_sel_cliente != null)
				rs_sel_cliente.close();
			if (rs_sel_medico != null)
				rs_sel_medico.close();
			if (rs_sel_consulta != null)
				rs_sel_consulta.close();
			if (rs_sel_anulacion != null)
				rs_sel_anulacion.close();
			if (pst_sel_cliente != null)
				pst_sel_cliente.close();
			if (pst_sel_medico != null)
				pst_sel_medico.close();
			if (pst_sel_consulta != null)
				pst_sel_consulta.close();
			if (pst_sel_anulacion != null)
				pst_sel_anulacion.close();
			if (pst_ins_consulta != null)
				pst_ins_consulta.close();
			if (pst_upd_medico != null)
				pst_upd_medico.close();
			if (con != null)
				con.close();
		}

	}

	/**
	 * anular_consulta: Método que implementa la transacción para anular una
	 * consulta. Recibe el NIF del cliente, el NIF del médico, la fecha de la
	 * consulta que se quiere anular, la fecha en la que se anula y el motivo. Si
	 * alguno de los datos no se encuentra en la base de datos, la fecha de
	 * anulación está a menos de dos días de la consulta que se quiere anular o el
	 * motivo es null, lanza una GestionMedicosException.
	 * 
	 * @param m_NIF_cliente     NIF del cliente
	 * @param m_NIF_medico      NIF del médico
	 * @param m_Fecha_Consulta  Fecha de la consulta a anular
	 * @param m_Fecha_Anulacion Fecha en la que se anula
	 * @param motivo            Motivo de la anulación
	 * @throws SQLException
	 */
	public static void anular_consulta(String m_NIF_cliente, String m_NIF_medico, Date m_Fecha_Consulta,
			Date m_Fecha_Anulacion, String motivo) throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		PreparedStatement pst_sel_cliente = null;
		PreparedStatement pst_sel_medico = null;
		PreparedStatement pst_sel_consulta = null;
		PreparedStatement pst_sel_anulacion = null;
		PreparedStatement pst_upd_medico = null;
		PreparedStatement pst_ins_anulacion = null;
		ResultSet rs_sel_cliente = null;
		ResultSet rs_sel_medico = null;
		ResultSet rs_sel_consulta = null;
		ResultSet rs_sel_anulacion = null;

		try {
			con = pool.getConnection();

			if (motivo == null) {
				throw new GestionMedicosException(6);
			}

			pst_sel_cliente = con.prepareStatement("select NIF from cliente where NIF=?");
			pst_sel_cliente.setString(1, m_NIF_cliente);
			rs_sel_cliente = pst_sel_cliente.executeQuery();

			if (!rs_sel_cliente.next()) {
				throw new GestionMedicosException(1);
			}

			pst_sel_medico = con.prepareStatement("select id_medico " + "from medico " + "where NIF=?");
			pst_sel_medico.setString(1, m_NIF_medico);
			rs_sel_medico = pst_sel_medico.executeQuery();

			if (!rs_sel_medico.next()) {
				throw new GestionMedicosException(2);
			}

			int idMedico = rs_sel_medico.getInt(1);
			java.sql.Date m_Fecha_sql = new java.sql.Date(m_Fecha_Consulta.getTime());

			if (Misc.howManyDaysBetween(m_Fecha_Consulta, m_Fecha_Anulacion) < 2) {
				throw new GestionMedicosException(5);
			}

			pst_sel_consulta = con.prepareStatement("select id_consulta from consulta where fecha_consulta=?");
			pst_sel_consulta.setDate(1, m_Fecha_sql);
			rs_sel_consulta = pst_sel_consulta.executeQuery();

			if (!rs_sel_consulta.next()) {
				throw new GestionMedicosException(4); // No existe consultas para ese día anuladas o no.
			}

			int idConsulta = rs_sel_consulta.getInt(1);
			pst_ins_anulacion = con.prepareStatement("insert into anulacion values (seq_anulacion.nextval, ?, ?, ?)");
			java.sql.Date m_Fecha_Anulacion_sql = new java.sql.Date(m_Fecha_Anulacion.getTime());
			pst_ins_anulacion.setInt(1, idConsulta);
			pst_ins_anulacion.setDate(2, m_Fecha_Anulacion_sql);
			pst_ins_anulacion.setString(3, motivo);
			pst_ins_anulacion.executeUpdate();

			pst_upd_medico = con.prepareStatement("update medico set consultas = consultas + ? where id_medico = ?"
					+ " and (select count(*) from consulta join anulacion on consulta.id_consulta=anulacion.id_consulta"
					+ " where fecha_consulta=? and consulta.id_medico = ?)"
					+ "=(select count(*) from consulta where fecha_consulta=? and id_medico=?)");
			pst_upd_medico.setInt(1, -1);
			pst_upd_medico.setInt(2, idMedico);
			pst_upd_medico.setDate(3, m_Fecha_sql);
			pst_upd_medico.setInt(4, idMedico);
			pst_upd_medico.setDate(5, m_Fecha_sql);
			pst_upd_medico.setInt(6, idMedico);
			int numUpd = pst_upd_medico.executeUpdate();

			if (numUpd == 0) {
				throw new GestionMedicosException(5); // Ya está anulada
			}

			con.commit();

		} catch (SQLException e) {

			con.rollback();

			if (e instanceof GestionMedicosException) {
				throw (GestionMedicosException) e;
			}
			if (new OracleSGBDErrorUtil().checkExceptionToCode(e, SGBDError.FK_VIOLATED)) {
				throw new GestionMedicosException(4); // Por si aparece una violacion de FK en el insert por no existir
														// consulta. Aunque debería de haberse dado cuenta antes.
			}

			logger.error(e.getMessage());
			throw e;

		} finally {
			/* Liberar recursos */
			if (rs_sel_cliente != null)
				rs_sel_cliente.close();
			if (rs_sel_medico != null)
				rs_sel_medico.close();
			if (rs_sel_consulta != null)
				rs_sel_consulta.close();
			if (rs_sel_anulacion != null)
				rs_sel_anulacion.close();
			if (pst_sel_cliente != null)
				pst_sel_cliente.close();
			if (pst_sel_medico != null)
				pst_sel_medico.close();
			if (pst_sel_consulta != null)
				pst_sel_consulta.close();
			if (pst_sel_anulacion != null)
				pst_sel_anulacion.close();
			if (pst_ins_anulacion != null)
				pst_ins_anulacion.close();
			if (pst_upd_medico != null)
				pst_upd_medico.close();
			if (con != null)
				con.close();
		}
	}

	/**
	 * consulta_medico: Método que imprime en pantalla las consultas no anuladas
	 * para un médico. Se le pasa el NIF del médico. Si el NIF no está en la base de
	 * datos lanza una GestionMedicosException. Imprime una consulta en cada línea.
	 * 
	 * @param m_NIF_medico NIF del médico
	 * @throws SQLException
	 */
	public static void consulta_medico(String m_NIF_medico) throws SQLException {

		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con = null;
		java.util.Date fecha;
		int consulta;
		int medico;
		String paciente;
		PreparedStatement pst_sel_medico = null;
		PreparedStatement pst_sel_consulta = null;
		ResultSet rs_sel_consulta = null;
		ResultSet rs_sel_medico = null;

		try {

			con = pool.getConnection();

			pst_sel_medico = con.prepareStatement("select id_medico " + "from medico " + "where NIF=?");
			pst_sel_medico.setString(1, m_NIF_medico);
			rs_sel_medico = pst_sel_medico.executeQuery();

			if (!rs_sel_medico.next()) {
				throw new GestionMedicosException(2);
			}

			medico = rs_sel_medico.getInt(1);

			pst_sel_consulta = con.prepareStatement("SELECT id_consulta, fecha_consulta, id_medico, NIF "
					+ "FROM consulta " + "WHERE id_medico = ? " + "AND NOT EXISTS ( " + "SELECT * " + "FROM anulacion "
					+ "WHERE anulacion.id_consulta = consulta.id_consulta " + ") " + "ORDER BY fecha_consulta");

			pst_sel_consulta.setInt(1, medico);
			rs_sel_consulta = pst_sel_consulta.executeQuery();

			System.out.println("Consultas para el médico " + medico);
			
			while (rs_sel_consulta.next()) {				
				fecha = rs_sel_consulta.getDate(2);
				consulta = rs_sel_consulta.getInt(1);
				paciente = new String(rs_sel_consulta.getString(4));
				System.out.println("*Fecha: " + fecha + "   *Consulta: " + consulta + "   *NIF Paciente: " + paciente);
			}
			
			con.commit();
			
		} catch (SQLException e) {

			con.rollback();
			logger.error(e.getMessage());
			throw e;

		} finally {
			/* Liberar recursos */

			if (rs_sel_medico != null)
				rs_sel_medico.close();
			if (rs_sel_consulta != null)
				rs_sel_consulta.close();
			if (pst_sel_medico != null)
				pst_sel_medico.close();
			if (pst_sel_consulta != null)
				pst_sel_consulta.close();
			if (con != null)
				con.close();
		}
	}
	
	/**
	 * crearTablas: Método que ejecuta el script gestion_medicos.sql.
	 * El script crea las tablas y secuencias de la base de datos.
	 */
	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_medicos.sql");
	}

	/**
	 * tests: Método que ejecuta diferentes tests sobre las transacciones reservar_consulta, anular_consulta y consulta_medico
	 * para comprobar su correcto funcionamiento. 
	 * Al inicio de cada test se llama al procedimiento inicializa_test definido en gestion_medicos.sql. 
	 * Este procedimiento elimina las posibles filas de cada tabla de la BD y las inicializa con algunos datos de prueba.
	 * 
	 * @throws SQLException
	 */
	static void tests() throws SQLException {
		creaTablas();

		PoolDeConexiones pool = PoolDeConexiones.getInstance();

		

		CallableStatement cll_reinicia = null;
		Connection conn = null;
		PreparedStatement pst_sel_reservas = null;
		ResultSet rs_sel_reservas = null;
		PreparedStatement pst_sel_anula = null;
		ResultSet rs_sel_anula = null;
		PreparedStatement pst_consultas_medico = null;
		ResultSet rs_consultas_medico = null;
		Date fechaBien = new Date(124, 3, 25); // Año 1900 + 124 = 2024. Mes empieza en 0. Día 25.
		Date fechaOcupada = new Date(122, 2, 25); // 25/03/2022 ocupada para médico con id = 2.
		Date fechaAnulada = new Date(123, 2, 24); // Esta consulta se añade a consultas y posteriormente se añade la
													// anulación de esta consulta a anulaciones.
		Date fechaInexistente = new Date(150, 5, 12); // Fecha que no existe en la BD.
		Date fechaAnulacionBien = new Date(122, 2, 20); // Fecha anulacion para anular consulta del 25/03/2022
		Date fechaAnulacionMal = new Date(122, 2, 24); // Fecha anulacion no valida para anular consulta del 25/03/2022
		java.sql.Date fechaBien_sql = new java.sql.Date(fechaBien.getTime());
		String motivoBien = "Me he curado milagrosamente";
		String motivoMal = null;
		int consultasFin;
		int consultasIni;
		java.sql.Date fechaAnulacionBien_sql = new java.sql.Date(fechaAnulacionBien.getTime());
		boolean insertBien = false;
		boolean consultas = false;

		// reservar_consulta con NIF del cliente no existente, NIF medico OK y fecha no
		// ocupada.
		// Debe darse cuenta de que no existe el cliente.
		try {
			// Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			reservar_consulta("12341234G", "222222B", fechaBien);
			System.out.println("RESERVA-MAL. No se da cuenta de que el NIF de cliente no existe.");
		} catch (SQLException e) {
			// System.out.println("Cod. Error: " + e.getErrorCode());
			if (e.getErrorCode() == 1) {
				System.out.println("RESERVA-OK. Se da cuenta de que NIF cliente no existe.");
			}
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();

		}

		// Reservar consulta con NIF cliente Ok, NIF médico MAL, fecha OK.
		// Debe darse cuenta de que NIF médico no existe.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			reservar_consulta("12345678A", "222288B", fechaBien);
			System.out.println("RESERVA-MAL. No se da cuenta de que el NIF de médico no existe.");
		} catch (SQLException e) {
			// System.out.println("Cod. Error: " + e.getErrorCode());
			if (e.getErrorCode() == 2) {
				System.out.println("RESERVA-OK. Se da cuenta de que NIF médico no existe.");
			}
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}
		if (rs_sel_reservas != null)
			rs_sel_reservas.close();
		// Reservar consulta en fecha con médico ocupado.
		// Debe darse cuenta de que ya existe una consulta para esa fecha.
		// Mirando los insert del procedimiento inicializa_test, el médico
		// con id_medico = 1 es el médico con NIF 22222222B, el cual tiene una consulta
		// el 24/03/2023.
		// Esa fecha está asignada a la variable fechaOcupada.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			reservar_consulta("87654321B", "8766788Y", fechaOcupada);
			System.out.println(
					"RESERVA-MAL. No se da cuenta de que ya se tiene una consulta en esa fecha para ese médico.");
		} catch (SQLException e) {
			// System.out.println("Cod. Error: " + e.getErrorCode());
			if (e.getErrorCode() == 3) {
				System.out.println(
						"RESERVA-OK. Se da cuenta de que ya existe una consulta en esa fecha para ese médico.");
			}
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Reservar consulta de una consulta reservada y anulada. Debe permitir la
		// reserva.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			reservar_consulta("12345678A", "222222B", fechaAnulada);
			System.out.println(
					"RESERVA-Ok. Se da cuenta de que ya se tiene una consulta en esa fecha para ese médico pero fue anulada.");
		} catch (SQLException e) {

			if (e.getErrorCode() == 3) {
				System.out.println(
						"RESERVA-Mal. No se da cuenta de que ya existe una consulta en esa fecha para ese médico pero fue anulada y la fecha está libre.");
			}
			logger.error(e.getMessage());
		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Reservar consulta con todos los datos OK. Debe insertar la consulta y
		// aumentar el contador de consultas del médico correspondiente.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			consultasIni = 0;
			consultasFin = 0;
			String nifMed = "222222B";
			insertBien = false;
			consultas = false;
			pst_consultas_medico = conn.prepareStatement("select consultas from medico where NIF =?");
			pst_consultas_medico.setString(1, nifMed);
			rs_consultas_medico = pst_consultas_medico.executeQuery();
			rs_consultas_medico.next();
			consultasIni = rs_consultas_medico.getInt(1);
			reservar_consulta("12345678A", nifMed, fechaBien);
			pst_sel_reservas = conn.prepareStatement("select * from consulta where fecha_consulta = ?");
			pst_sel_reservas.setDate(1, fechaBien_sql);
			rs_sel_reservas = pst_sel_reservas.executeQuery();
			if (rs_sel_reservas.next()) {
				insertBien = true;
			}
			if (rs_consultas_medico != null)
				rs_consultas_medico.close();
			pst_consultas_medico.setString(1, nifMed);
			rs_consultas_medico = pst_consultas_medico.executeQuery();
			rs_consultas_medico.next();
			consultasFin = rs_consultas_medico.getInt(1);
			if (consultasFin - consultasIni == 1) {
				consultas = true;
			}

			if (consultas && insertBien) {
				System.out
						.println("RESERVA-OK. Inserta la anulación y incrementa el contador de consultas del médico.");
			} else {
				System.out.println("RESERVA-Mal. No inserta la fila o no incrementa el contador correctamente.");
			}

		} catch (SQLException e) {
			System.out.println("RESERVA-Mal. Algo no ha ido bien en la transacción. Cod. error:" + e.getErrorCode());
			logger.error(e.getMessage());

		} finally {

			if (rs_consultas_medico != null)
				rs_consultas_medico.close();
			if (rs_sel_reservas != null)
				rs_sel_reservas.close();
			if (pst_consultas_medico != null)
				pst_consultas_medico.close();			
			if (pst_sel_reservas != null)
				pst_sel_reservas.close();
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Anular consulta con NIF cliente inexistente.Resto de datos OK.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			anular_consulta("123000A", "8766788Y", fechaOcupada, fechaAnulacionBien, motivoBien);
			System.out.println("ANULA-Mal. No se da cuenta de que el NIF cliente no existe.");
		} catch (SQLException e) {
			if (e.getErrorCode() == 1) {
				System.out.println("ANULA-OK. Se da cuenta de que el NIF de cliente no existe.");
			}
			logger.error(e.getMessage());

		} finally {

			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Anular consulta con NIF médico inexistente.Resto de datos OK.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			anular_consulta("87654321B", "8766700Y", fechaOcupada, fechaAnulacionBien, motivoBien);
			System.out.println("ANULA-Mal. No se da cuenta de que el NIF del médico no existe.");
		} catch (SQLException e) {
			if (e.getErrorCode() == 2) {
				System.out.println("ANULA-OK. Se da cuenta de que el NIF del médico no existe.");
			}
			logger.error(e.getMessage());

		} finally {

			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Anular consulta con fecha consulta inexistente.Resto de datos OK.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			anular_consulta("87654321B", "8766788Y", fechaInexistente, fechaAnulacionBien, motivoBien);
			System.out.println("ANULA-Mal. No se da cuenta de que la fecha de cosnulta no existe.");
		} catch (SQLException e) {
			if (e.getErrorCode() == 4) {
				System.out.println("ANULA-OK. Se da cuenta de que la fecha de consulta no existe.");
			}
			logger.error(e.getMessage());

		} finally {

			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Anular consulta con fecha anulacion a menos de dos días.Resto de datos OK.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			anular_consulta("87654321B", "8766788Y", fechaOcupada, fechaAnulacionMal, motivoBien);
			System.out.println("ANULA-Mal. No se da cuenta de que la fecha de cosnulta no existe.");
		} catch (SQLException e) {
			if (e.getErrorCode() == 5) {
				System.out.println(
						"ANULA-OK. Se da cuenta de que la fecha de anulación esta a menos de dos días de la consulta.");
			}
			logger.error(e.getMessage());

		} finally {

			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Anular consulta con motivo a null.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			anular_consulta("87654321B", "8766788Y", fechaOcupada, fechaAnulacionBien, motivoMal);
			System.out.println("ANULA-Mal. No se da cuenta de que la fecha de cosnulta no existe.");
		} catch (SQLException e) {
			if (e.getErrorCode() == 6) {
				System.out.println("ANULA-OK. Se da cuenta de que el motivo es null.");
			}
			logger.error(e.getMessage());

		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Anular consulta con todos los datos OK. Debe insertar la anulación y
		// decrementar el contador de consultas del médico correspondiente.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			consultasIni = 0;
			consultasFin = 0;
			String nifMedico = "8766788Y";
			insertBien = false;
			consultas = false;
			pst_consultas_medico = conn.prepareStatement("select consultas from medico where NIF =?");
			pst_consultas_medico.setString(1, nifMedico);
			rs_consultas_medico = pst_consultas_medico.executeQuery();
			rs_consultas_medico.next();
			consultasIni = rs_consultas_medico.getInt(1);
			anular_consulta("12345678A", nifMedico, fechaOcupada, fechaAnulacionBien, motivoBien);
			pst_sel_anula = conn.prepareStatement("select * from anulacion where fecha_anulacion = ?"); // No es la
																										// mejor forma,
																										// pero sabemos
																										// que no
																										// tenemos esa
																										// fecha de
																										// anulación en
																										// la BD.
			pst_sel_anula.setDate(1, fechaAnulacionBien_sql);
			rs_sel_anula = pst_sel_anula.executeQuery();
			if (rs_sel_anula.next()) {
				insertBien = true;
			}
			if (rs_consultas_medico != null)
				rs_consultas_medico.close();
			pst_consultas_medico.setString(1, nifMedico);
			rs_consultas_medico = pst_consultas_medico.executeQuery();
			rs_consultas_medico.next();
			consultasFin = rs_consultas_medico.getInt(1);
			if (consultasIni - consultasFin == 1) {
				consultas = true;
			}

			if (consultas && insertBien) {
				System.out.println("ANULA-OK. Inserta la anulación y decrementa el contador de consultas.");
			} else {
				System.out.println("ANULA-Mal. No inserta la anulación y/o no decrementa el contador de consultas.");
			}

		} catch (SQLException e) {
			System.out.println("ANULA-Mal. Algo no ha ido bien en la transacción. Cod. error:" + e.getErrorCode());
			logger.error(e.getMessage());

		} finally {
			if (rs_sel_reservas != null)
				rs_sel_reservas.close();
			if(rs_sel_anula != null)
				rs_sel_anula.close();
			if(pst_sel_anula != null)
				pst_sel_anula.close();
			if (pst_sel_reservas != null)
				pst_sel_reservas.close();
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Consultar médico con NIF medico inexistente
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			consulta_medico("121212B");
			System.out.println("CONSULTA-Mal. No se da cuenta de que el NIF  del médico no existe.");
		} catch (SQLException e) {
			if (e.getErrorCode() == 2) {
				System.out.println("CONSULTA-OK. Se da cuenta de que el NIF del médico no existe.");
			}
			logger.error(e.getMessage());

		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

		// Consultar consultar todo OK.
		try {
			// Reinicio filas

			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream(baos);
			System.setOut(ps); // Redirigir la salida a ps.
			consulta_medico("8766788Y");
			String salida = baos.toString().trim(); // Convertir la salida a cadena y eliminar blancos.
			String expected = "Consultas para el médico 2\n"
					+ "*Fecha: 2022-03-25   *Consulta: 2   *NIF Paciente: 87654321B"; // Cadena esperada.
			System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out))); // redirigir la salida de nuevo a
																						// la salida estándar.
			try {
				assertEquals(expected, salida.trim()); // Comparacion salida esperada y real. Se hace trim para eliminar
														// blancos y no obtener errores por un espacio de más, por
														// ejemplo.
				System.out.println("CONSULTA-OK. Muestra la salida esperada.");
			} catch (AssertionError ex) {
				logger.error(ex.getMessage());
				System.out.println("Mal. La salida real y la esperada no coinciden.");
				throw ex;
			}
		} catch (SQLException e) {

			logger.error(e.getMessage());

		} finally {
			if (cll_reinicia != null)
				cll_reinicia.close();
			if (conn != null)
				conn.close();
		}

	}

}
