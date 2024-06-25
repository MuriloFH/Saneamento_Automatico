from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def distancia(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
              corrigirErros=False,
              km_distancia_nulo=False
              ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_km_distancia_nulo(pre_validacao):
        nomeValidacao = "Kilometragem não informado para a distância."

        def analisa_km_distancia_nulo():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_km_distancia_nulo(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""select *
                                            from bethadba.distancias
                                            where total_km is null
                                        """)

                if len(busca) > 0:
                    for row in busca:
                        dadoAlterado.append(f"Inserido a kilometragem 1 para a distância {row['i_distancias']}")
                        comandoUpdate += f"""UPDATE bethadba.distancias set total_km = 1 where i_distancias = {row['i_distancias']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha", tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_km_distancia_nulo: {e}")
            return

        if km_distancia_nulo:
            dado = analisa_km_distancia_nulo()

            if corrigirErros and len(dado) > 0:
                corrige_km_distancia_nulo(listDados=dado)

    if dadosList:
        analisa_corrige_km_distancia_nulo(pre_validacao="km_distancia_nulo")
