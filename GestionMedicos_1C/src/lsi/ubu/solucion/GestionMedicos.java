package lsi.ubu.solucion;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lsi.ubu.util.ExecuteScript;
import lsi.ubu.util.PoolDeConexiones;


/**
 * GestionMedicos:
 * Implementa la gestion de medicos segun el PDF de la carpeta enunciado
 * 
 * @author <a href="mailto:jmaudes@ubu.es">Jesus Maudes</a>
 * @author <a href="mailto:rmartico@ubu.es">Raul Marticorena</a>
 * @author <a href="mailto:pgdiaz@ubu.es">Pablo Garcia</a>
 * @version 1.0
 * @since 1.0 
 */
public class GestionMedicos {
	
	private static Logger logger = LoggerFactory.getLogger(GestionMedicos.class);

	private static final String script_path = "sql/";

	public static void main(String[] args) throws SQLException{		
		tests();

		System.out.println("FIN.............");
	}
	
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
	
	static public void creaTablas() {
		ExecuteScript.run(script_path + "gestion_medicos.sql");
	}

	static void tests() throws SQLException{
		creaTablas();
		
		PoolDeConexiones pool = PoolDeConexiones.getInstance();		
		
		//Relatar caso por caso utilizando el siguiente procedure para inicializar los datos
		
		CallableStatement cll_reinicia=null;
		Connection conn = null;
		
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
		
	}
}
