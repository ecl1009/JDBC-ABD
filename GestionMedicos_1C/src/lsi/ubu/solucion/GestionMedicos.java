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
	
	public static void consulta_medico(String m_NIF_medico)
			throws SQLException {

				
		PoolDeConexiones pool = PoolDeConexiones.getInstance();
		Connection con=null;

	
		try{
			con = pool.getConnection();
			
		} catch (SQLException e) {
			//Completar por el alumno			
			
			logger.error(e.getMessage());
			throw e;		

		} finally {
			/*A rellenar por el alumno, liberar recursos*/
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
		
		try {
			//Reinicio filas
			conn = pool.getConnection();
			cll_reinicia = conn.prepareCall("{call inicializa_test}");
			cll_reinicia.execute();
		} catch (SQLException e) {				
			logger.error(e.getMessage());			
		} finally {
			if (cll_reinicia!=null) cll_reinicia.close();
			if (conn!=null) conn.close();
		
		}			
		
	}
}
