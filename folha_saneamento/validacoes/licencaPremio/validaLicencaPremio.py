from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def licencaPremio(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  faixa_maior_99=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_faixa_maior_99(pre_validacao):
        nomeValidacao = "Faixa da configuração da licença prémio maior que 99"

        def analisa_faixa_maior_99():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_faixa_maior_99(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select i_licpremio_config, i_faixas
                                            from bethadba.licpremio_faixas 
                                            where i_faixas > 99
                                        """)

                for row in dados:
                    dadoAlterado.append(f"Alterado a faixa para 99 da configuração de licença prémio {row['i_licpremio_config']}")
                    comandoUpdate += f"""UPDATE bethadba.licpremio_faixas set i_faixas = 99 where i_licpremio_config = {row['i_licpremio_config']} and i_faixas = {row['i_faixas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_faixa_maior_99: {e}")
            return

        if faixa_maior_99:
            dado = analisa_faixa_maior_99()

            if corrigirErros and len(dado) > 0:
                corrige_faixa_maior_99(listDados=dado)

    if dadosList:
        analisa_corrige_faixa_maior_99(pre_validacao="faixa_maior_99")
