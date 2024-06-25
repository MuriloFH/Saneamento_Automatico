from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta



def caracteristica(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                 corrigirErros=False,
                 observacao_maior_150_caracteres=False
                 ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_observacao_maior_150_caracteres(pre_validacao):
        nomeValidacao = "Características com observação maior do que 150 caracteres."

        def analisa_observacao_maior_150_caracteres():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_observacao_maior_150_caracteres(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        i_caracteristicas, 
                        nome,
                        observacao 
                    FROM bethadba.caracteristicas 
                    where LENGTH(observacao) > 150   
                 """)

                if len(busca) > 0:

                    for row in busca:
                        dadoAlterado.append(f"Alterada observação da caracteristica {row['i_caracteristicas']}-{row['nome']} para {row['observacao'][:150]}")
                        comandoUpdate += f"""UPDATE bethadba.caracteristicas set observacao = '{row['observacao'][:150]}' where i_caracteristicas = {row['i_caracteristicas']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_observacao_maior_150_caracteres: {e}")
            return

        if observacao_maior_150_caracteres:
            dado = analisa_observacao_maior_150_caracteres()

            if corrigirErros and len(dado) > 0:
                corrige_observacao_maior_150_caracteres(listDados=dado)

    if dadosList:
        analisa_corrige_observacao_maior_150_caracteres(pre_validacao="observacao_maior_150_caracteres")
