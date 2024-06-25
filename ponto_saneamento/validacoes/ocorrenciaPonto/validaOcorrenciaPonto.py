from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def ocorrenciaPonto(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                    corrigirErros=False,
                    descricao_duplicada=False
                    ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_descricao_duplicada(pre_validacao):
        nomeValidacao = "Descrição da ocorrência duplicada"

        def analisa_descricao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""SELECT 
                                                LIST(i_ocorrencias_ponto) as ocorrencias_ponto, 
                                                TRIM(nome) as descricao, 
                                                COUNT(nome) as qtdDuplicada
                                            FROM 
                                                bethadba.ocorrencias_ponto
                                            GROUP BY 
                                                descricao
                                            HAVING 
                                               qtdDuplicada > 1
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        listOcorrencias = row['ocorrencias_ponto'].split(',')

                        for i in range(0, len(listOcorrencias)):
                            newDescricao = f"{row['descricao']} | {listOcorrencias[i]}"

                            dadoAlterado.append(f"Alterado a descrição da ocorrência {listOcorrencias[i]} para {newDescricao}")
                            comandoUpdate += f"UPDATE bethadba.ocorrencias_ponto set nome = '{newDescricao}' where i_ocorrencias_ponto in ({listOcorrencias[i]});\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_descricao_duplicada: {e}")
            return

        if descricao_duplicada:
            dado = analisa_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_descricao_duplicada(listDados=dado)

    if dadosList:
        analisa_corrige_descricao_duplicada(pre_validacao="descricao_duplicada")
