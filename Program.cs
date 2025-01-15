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

        string configPathNewBd = Path.Combine(Directory.GetCurrentDirectory(), "..", "BANCODEDADOS2.FDB");
        string novoConfBd = $"User=SYSDBA;Password=masterkey;Database={configPathNewBd};DataSource=127.0.0.1;Port=3050;Dialect=3";

        try {
            using (FbConnection connect = new FbConnection(confBD)) {
                // abrinco conexão
                await connect.OpenAsync().ConfigureAwait(false);

                string query = "SELECT * FROM MORADORES";
                string query2 = "SELECT * FROM CID_USUARIOS";

                // Criando FbCommand Moradores
                FbCommand moradores = new FbCommand(query, connect);
                FbDataReader readerMoradores = await moradores.ExecuteReaderAsync().ConfigureAwait(false);

                // Criando FbCommand CID_USUARIOS
                FbCommand cid_usuarios = new FbCommand(query2, connect);
                FbDataReader readerCidUsuarios = await cid_usuarios.ExecuteReaderAsync().ConfigureAwait(false);


                // Lista para armazenar as linhas que passarem na verificação
                List<Dictionary<string, object>> moradoresComTV = new List<Dictionary<string, object>>();

                // Processar a tabela de MORADORES
                while (await readerMoradores.ReadAsync().ConfigureAwait(false))
                {
                    var unidade = readerMoradores["UNIDADE"]?.ToString();

                    // Verificar se "TV" está contido no campo UNIDADE
                    if (!string.IsNullOrEmpty(unidade) && unidade.IndexOf("TV", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        var linhaMorador = new Dictionary<string, object>();
                        for (int i = 0; i < readerMoradores.FieldCount; i++)
                        {
                            linhaMorador[readerMoradores.GetName(i)] = readerMoradores.GetValue(i);
                        }
                        moradoresComTV.Add(linhaMorador);
                    }
                }

                // Extrair os IDs de moradores com "TV"
                var moradoresComTVIds = moradoresComTV.Select(m => m["IDMORADOR"]).Cast<int>().ToList();

                // Lista para armazenar os registros de CID_USUARIOS filtrados
                List<Dictionary<string, object>> cidUsuariosFiltrados = new List<Dictionary<string, object>>();

                 // Processar a tabela de CID_USUARIOS
                while (await readerCidUsuarios.ReadAsync().ConfigureAwait(false))
                {
                    var idUsuario = readerCidUsuarios["ID_USUARIO"] as int?;
                    if (idUsuario.HasValue && moradoresComTVIds.Contains(idUsuario.Value))
                    {
                        var linhaCidUsuario = new Dictionary<string, object>();
                        for (int i = 0; i < readerCidUsuarios.FieldCount; i++)
                        {
                            linhaCidUsuario[readerCidUsuarios.GetName(i)] = readerCidUsuarios.GetValue(i);
                        }
                        cidUsuariosFiltrados.Add(linhaCidUsuario);
                    }
                }

                using (FbConnection novoConnect = new FbConnection(novoConfBd)) {
                    await novoConnect.OpenAsync().ConfigureAwait(false);
                    
                    // Inserir os moradores com "TV"
                    foreach (var morador in moradoresComTV)
                    {
                        var insertMoradorQuery = GenerateInsertQuery("MORADORES", morador);
                        FbCommand cmdInsertMorador = new FbCommand(insertMoradorQuery, novoConnect);

                        // Adicionar parâmetros ao comando dinamicamente
                        foreach (var item in morador)
                        {
                            cmdInsertMorador.Parameters.AddWithValue($"@{item.Key}", item.Value);
                        }

                        await cmdInsertMorador.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    // Inserir os registros de CID_USUARIOS
                    foreach (var usuario in cidUsuariosFiltrados)
                    {
                        var insertCidUsuarioQuery = GenerateInsertQuery("CID_USUARIOS", usuario);
                        FbCommand cmdInsertCidUsuario = new FbCommand(insertCidUsuarioQuery, novoConnect);

                        // Adicionar parâmetros ao comando dinamicamente
                        foreach (var item in usuario)
                        {
                            cmdInsertCidUsuario.Parameters.AddWithValue($"@{item.Key}", item.Value);
                        }

                        await cmdInsertCidUsuario.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                }
            }
        } catch(Exception error) {
            Console.WriteLine(error);
        }
    }

    // Método para gerar a query INSERT dinamicamente
    private static string GenerateInsertQuery(string tableName, Dictionary<string, object> data)
    {
        var columns = string.Join(", ", data.Keys);
        var parameters = string.Join(", ", data.Keys.Select(k => $"@{k}"));
        return $"INSERT INTO {tableName} ({columns}) VALUES ({parameters})";
    }
}