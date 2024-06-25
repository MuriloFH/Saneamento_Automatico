from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import geraCnpj


def configuracaoRaisCampo(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                          corrigirErros=False,
                          cnpj_nulo=False
                          ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_cnpj_nulo(pre_validacao):
        nomeValidacao = "Cnpj nulo"

        def analisa_cnpj_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_cnpj_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            newDtHomologacao = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select rc.campo
                                            from bethadba.rais_campos rc
                                            where EXISTS (select 1 from bethadba.rais_eventos re where re.campo = rc.campo)
                                            and rc.cnpj is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        newCnpj = geraCnpj()
                        dadoAlterado.append(f"Inserido ")
                        comandoUpdate += f"UPDATE bethadba.rais_campos set cnpj = '{newCnpj}' where campo = {row['campo']};\n"

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_controle_ponto_nulo: {e}")
            return

        if cnpj_nulo:
            dado = analisa_cnpj_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_cnpj_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_cnpj_nulo(pre_validacao="cnpj_nulo")
