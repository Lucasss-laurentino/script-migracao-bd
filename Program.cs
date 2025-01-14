using FirebirdSql.Data.FirebirdClient; // importação necessária pra trabalhar com firebird
using System;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;
using System.IO;

class IntegrationToFirebird {

    static async Task Main() {

        string configPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "BANCODEDADOS.FDB");
        string confBD = $"User=SYSDBA;Password=masterkey;Database={configPath};DataSource=127.0.0.1;Port=3050;Dialect=3";

        try {
            using (FbConnection connect = new FbConnection(confBD)) {
                // abrinco conexão
                await connect.OpenAsync().ConfigureAwait(false);

                string query = "SELECT * FROM MORADORES WHERE UNIDADE LIKE '%TV%'";
                string query2 = "SELECT * FROM CID_USUARIOS";

                // Criando FbCommand Moradores
                FbCommand moradores = new FbCommand(query, connect);
                FbDataReader readerMoradores = await moradores.ExecuteReaderAsync().ConfigureAwait(false);

                // Criando FbCommand CID_USUARIOS
                FbCommand cid_usuarios = new FbCommand(query2, connect);
                FbDataReader readerCidUsuarios = await cid_usuarios.ExecuteReaderAsync().ConfigureAwait(false);

                List<int> moradoresID = new List<int>();
                List<int> cidUsuariosID = new List<int>();

                while(await readerMoradores.ReadAsync().ConfigureAwait(false)) {
                    moradoresID.Add((int)readerMoradores["IDMORADOR"]);
                }
                
                while(await readerCidUsuarios.ReadAsync().ConfigureAwait(false)) {
                    cidUsuariosID.Add((int)readerCidUsuarios["ID_USUARIO"]);
                }

                 // Filtrando os IDs de CID_USUARIOS que não estão na lista de MORADORES
                List<int> usuariosExcluir = cidUsuariosID.Except(moradoresID).ToList();

                int count = 0;
                // Exibindo os IDs que serão excluídos
                if (usuariosExcluir.Any())
                {
                    foreach (var id in usuariosExcluir)
                    {
                        Console.WriteLine($"Excluindo: {id}");

                        using(FbTransaction transaction = connect.BeginTransaction()) {
                            // excluindo de cid_usuarios
                            string query_excluir_CID_USUARIOS = $"DELETE FROM CID_USUARIOS WHERE ID_USUARIO = {id}";
                            FbCommand excluir_CID_USUARIOS = new FbCommand(query_excluir_CID_USUARIOS, connect, transaction);
                            await excluir_CID_USUARIOS.ExecuteNonQueryAsync().ConfigureAwait(false);

                            transaction.Commit();
                        }

                        using(FbTransaction transaction = connect.BeginTransaction()) {
                            // excluindo de moradores
                            string query_excluir_moradores = $"DELETE FROM MORADORES WHERE IDMORADOR = {id}";
                            FbCommand excluir_moradores = new FbCommand(query_excluir_moradores, connect, transaction);
                            await excluir_moradores.ExecuteNonQueryAsync().ConfigureAwait(false);

                            transaction.Commit();
                        }

                        count++;
                    }
                }
                else
                {
                    Console.WriteLine("Nenhum usuário para excluir.");
                }

                Console.WriteLine($"usuarios excluidos: {count}");

            }
        } catch(Exception error) {
            Console.WriteLine(error);
        }
    }
}