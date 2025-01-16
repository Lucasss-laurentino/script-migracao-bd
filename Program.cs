using FirebirdSql.Data.FirebirdClient;
using System.Linq;
using System.Threading.Tasks;
using System.Data.Common;
using System.IO;
using System.Collections.Generic;
using System.Diagnostics;

class IntegrationToFirebird {

    static async Task Main() {

        string configPath = Path.Combine(Directory.GetCurrentDirectory(), "..", "BANCODEDADOS2.FDB");
        string confBD = $"User=SYSDBA;Password=masterkey;Database={configPath};DataSource=127.0.0.1;Port=3050;Dialect=3";

        string configPathNewBd = Path.Combine(Directory.GetCurrentDirectory(), "..", "BANCODEDADOS.FDB");
        string novoConfBd = $"User=SYSDBA;Password=masterkey;Database={configPathNewBd};DataSource=127.0.0.1;Port=3050;Dialect=3";

        try {
            // filtrando dados banco 1
            using (FbConnection connect = new FbConnection(confBD)) {
                // abrindo conexão
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

                // Inserindo dados no banco 2
                using (FbConnection novoConnect = new FbConnection(novoConfBd)) {
                    await novoConnect.OpenAsync().ConfigureAwait(false);
                    
                    // excluindo dados duplicados
                    moradoresComTV = moradoresComTV.Distinct().ToList();

                    // Obter os campos válidos da tabela "MORADORES"
                    List<string> camposValidos = ObterCamposTabela(novoConnect, "MORADORES");

                    // Inserir os moradores com "TV"
                    foreach (var morador in moradoresComTV)
                    {
                         // Filtrar somente os campos que existem na tabela
                        var moradorFiltrado = morador
                            .Where(kvp => camposValidos.Contains(kvp.Key.ToUpper())) // Certifique-se de comparar em maiúsculas
                            .ToDictionary(kvp => kvp.Key, kvp => kvp.Value);

                        // Gerar a query com os campos filtrados
                        var insertMoradorQuery = GenerateInsertQuery("MORADORES", moradorFiltrado);
                        FbCommand cmdInsertMorador = new FbCommand(insertMoradorQuery, novoConnect);

                        // Adicionar os parâmetros ao comando
                        foreach (var item in moradorFiltrado)
                        {
                            if(item.Key == "UNIDADE") {
                                string unidade = (string)item.Value;
                                string unidadeChanged = unidade.Replace("TV", "BLOCO 1");
                                cmdInsertMorador.Parameters.AddWithValue($"@{item.Key}", unidadeChanged);
                            } else {
                                cmdInsertMorador.Parameters.AddWithValue($"@{item.Key}", item.Value);
                            }
                        }

                        // Executar o comando
                        await cmdInsertMorador.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }

                    // // Inserir os registros de CID_USUARIOS
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

                    // Inserir unidades
                    foreach (var linhaMorador in moradoresComTV) {
                        // filtrar unidades
                        string? unidade = linhaMorador["UNIDADE"]?.ToString()?.Replace("TV", "BLOCO 1");
                        
                        int andar = 0;
                        int apt = 0;
                        
                        ExtrairAndarEApt(unidade != null ? unidade : "", out andar, out apt);
                        
                        string timestamp = GerarTimestamp();
                        string queryInsertUnidade = $"INSERT INTO UNIDADE (NUMVAGAS, DATAHORACADASTRO, OPERADORCADASTRO, DATAHORAALTERACAO, OPERADORALTERACAO, OBS, UNIDADE, ANDAR_QUADRA, APTO_LOTE, BLOCO, VAGAS_OCUPADAS, NUMVAGAS2) VALUES (1, '{timestamp}', 'LINEAR', NULL, NULL, 'INSERIDO POR SCRIPT-CP', '{unidade}', {andar}, {apt}, 1, 0, NULL )";

                        FbCommand unidadeInsert = new FbCommand(queryInsertUnidade, novoConnect);           

                        await unidadeInsert.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    
                }
            }
         
            // feedback
           string mensagemConcluido = @"
            *************************************************************
            *                                                           *
            *                  SCRIPT CONCLUIDO COM SUCESSO!            *
            *                                                           *
            *************************************************************
                           |    
                          / \      
                         / _ \    
                        | (_) |   
                        |  _  |   
                        | (_) |   
                        |_____|   
                         |   |   
                         |   |   
                        /     \   
                       /_______\  
                      |  _____  |  
                      | |     | |   
                      | |     | |   
                      | |_____| |   
                     /___________\
            ============================================================
            |  O SCRIPT CONCLUIDO COM SUCESSO!                      |
            ============================================================
            ";


            // Salva a mensagem de erro em um arquivo temporário
            string tempFilePath = Path.Combine(Path.GetTempPath(), "erro_mensagem.txt");
            File.WriteAllText(tempFilePath, mensagemConcluido);

            ProcessStartInfo startInfo = new ProcessStartInfo()
            {
                FileName = "cmd.exe",
                Arguments = $"/k type \"{tempFilePath}\"",
                CreateNoWindow = false,  
                UseShellExecute = true
            };

            // Inicia o processo para abrir o terminal e mostrar a mensagem
            Process.Start(startInfo);
        
        
        } catch(Exception error) {
            // Para acessar detalhes do Firebird, como o código de erro, se for uma exceção específica de Firebird
            if (error is FirebirdSql.Data.FirebirdClient.FbException fbError)
            {
                // Acessando propriedades específicas do erro do Firebird
               if(fbError.ErrorCode == 335544344) {
                     // Desenhando "ERRO" de forma estilizada com Console.WriteLine
                    string mensagemErro = @"
                    EEEEE   RRRRR   RRRRR   OOOOOOO  
                    E       R    R  R    R  O     O 
                    EEEE    RRRRR   RRRRR   O     O 
                    E       R   R   R   R   O     O 
                    EEEEE   R    R  R    R  OOOOOOO  

                    =============================================
                    |            ERRO: Banco de Dados          |
                    |     Nao foi possivel acessar o banco.    |
                    =============================================
                    
                    SOLUCOES POSSIVEIS:

                    1. Verifique se os arquivos 'BANCODEDADOS.FDB' e 'BANCODEDADOS2.FDB' estao
                    no mesmo diretorio que o script.

                    2. O banco de dados 'BANCODEDADOS2.FDB' deve ser o banco filtrado, e
                    'BANCODEDADOS.FDB' deve ser o banco que recebera os novos dados.

                    =========================================================
                    Corrija o caminho ou nome dos arquivos e tente novamente.
                    =========================================================
                    ";

                    // Salva a mensagem de erro em um arquivo temporário
                    string tempFilePath = Path.Combine(Path.GetTempPath(), "erro_mensagem.txt");
                    File.WriteAllText(tempFilePath, mensagemErro);

                    ProcessStartInfo startInfo = new ProcessStartInfo()
                    {
                        FileName = "cmd.exe",
                        Arguments = $"/k type \"{tempFilePath}\"",
                        CreateNoWindow = false,  
                        UseShellExecute = true
                    };

                    // Inicia o processo para abrir o terminal e mostrar a mensagem
                    Process.Start(startInfo);
               }
            }
        }
    }

    // Método para gerar a query INSERT dinamicamente
    private static string GenerateInsertQuery(string tableName, Dictionary<string, object> data)
    {
        var columns = string.Join(", ", data.Keys);
        var parameters = string.Join(", ", data.Keys.Select(k => $"@{k}"));
        return $"INSERT INTO {tableName} ({columns}) VALUES ({parameters})";
    }

     static string GerarTimestamp()
    {
        return DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
    }

    static void ExtrairAndarEApt(string input, out int andar, out int apt)
    {
        // Inicializar as variáveis
        andar = 0;
        apt = 0;

        // Encontrar a posição da palavra "BLOCO"
        int blocoIndex = input.IndexOf("BLOCO");

        // Capturar os números antes de "BLOCO"
        string numerosAntesDeBloco = input.Substring(0, blocoIndex).Trim();

        // Verificar se há 4 ou mais dígitos antes da palavra "BLOCO"
        if (numerosAntesDeBloco.Length >= 4)
        {
            // Pegar os dois primeiros números como andar
            andar = int.Parse(numerosAntesDeBloco.Substring(0, 2));
        }
        else
        {
            // Pegar apenas o primeiro número como andar
            andar = int.Parse(numerosAntesDeBloco.Substring(0, 1));
        }

        // Extrair o último número antes de "BLOCO" como apt
        apt = int.Parse(numerosAntesDeBloco.Substring(numerosAntesDeBloco.Length - 1));
    }

    public static List<string> ObterCamposTabela(FbConnection connection, string tableName)
    {
        List<string> colunas = new List<string>();
        
        string query = @"
            SELECT TRIM(R.RDB$FIELD_NAME) AS FIELD_NAME
            FROM RDB$RELATION_FIELDS R
            WHERE R.RDB$RELATION_NAME = @TableName
            ORDER BY R.RDB$FIELD_POSITION";
        
        using (FbCommand cmd = new FbCommand(query, connection))
        {
            cmd.Parameters.AddWithValue("@TableName", tableName.ToUpper());
            
            using (FbDataReader reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    colunas.Add(reader["FIELD_NAME"].ToString());
                }
            }
        }

        return colunas;
    }

}