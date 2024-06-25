from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
from utilitarios.funcoesGenericas.funcoes import corrige_capacidade_volumetrica_nula


def veiculoCombustivel(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                       corrigirErros=False,
                       veiculo_sem_combustivel_padrao=False,
                       veiculo_sem_capacidade_volumetrica=False):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    tipoValidacao = "veiculoCombustivel"

    banco = Conecta(odbc=nomeOdbc)

    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def valida_corrige_veiculo_sem_combustivel_padrao(pre_validacao):

        nomeValidacao = "Veículo sem combustivel padrão informado"
        preValidacaoBanco = "veiculo_sem_combustivel_padrao"

        def analisa_veiculo_sem_combustivel_padrao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_veiculo_sem_combustivel_padrao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            if corrigirErros and len(listDados) > 0:
                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                listVeiculos = []

                newCombustivelPadrao = banco.consultar(f"""SELECT FIRST i_materiais, m.nome
                                                            from bethadba.materiais m
                                                            where m.combustivel = 'S' and m.tipo_combustivel = 2
                                                        """)[0]

                try:
                    for i in listDados:
                        listVeiculos.append(f"'{i['i_chave_dsk1']}'")

                        dadoAlterado.append(f"Adicionado o combustivel {newCombustivelPadrao['nome']} no veiculo {i['i_chave_dsk1']}")

                    iVeiculos = ", ".join(listVeiculos)
                    comandoUpdate += f"UPDATE bethadba.veiculos set i_materiais = {newCombustivelPadrao['i_materiais']} where i_veiculos in ({iVeiculos});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipoValidacao, preValidacaoBanco=preValidacaoBanco, dadoAlterado=dadoAlterado)

                    corrige_capacidade_volumetrica_nula(listVeiculos=iVeiculos, banco=banco)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função corrige_veiculo_sem_combustivel_padrao {e}")

        if veiculo_sem_combustivel_padrao:
            dado = analisa_veiculo_sem_combustivel_padrao()

            if corrigirErros and len(dado) > 0:
                corrige_veiculo_sem_combustivel_padrao(listDados=dado)

    def valida_corrige_veiculo_sem_capacidade_volumetrica(pre_validacao):
        nomeValidacao = "Veículo sem capacidade volumétrica"
        preValidacaoBanco = "veiculo_sem_capacidade_volumetrica"

        def analisa_veiculo_sem_capacidade_volumetrica():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            if len(dados) > 0:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")
            else:
                print(f">> Total de inconsistências encontradas: {len(dados)}")
                logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_veiculo_sem_capacidade_volumetrica(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            if corrigirErros and len(listDados) > 0:
                print(f">> Iniciando a correção '{nomeValidacao}' ")
                logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

                dadoAlterado = []
                listVeiculos = []

                newCapacidade = 50.0

                try:
                    for i in listDados:
                        listVeiculos.append(f"'{i['i_chave_dsk1']}'")

                        dadoAlterado.append(f"Adicionado a capacidade volumétrica {newCapacidade} no veiculo {i['i_chave_dsk1']}")

                    iVeiculos = ", ".join(listVeiculos)
                    comandoUpdate += f"UPDATE bethadba.veiculos set limite = {newCapacidade} where i_veiculos in ({iVeiculos});\n"

                    banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Frotas", tipoValidacao=tipoValidacao, preValidacaoBanco=preValidacaoBanco, dadoAlterado=dadoAlterado)

                    print(f">> Finalizado a correção '{nomeValidacao}'")
                    logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
                except Exception as e:
                    print(f"Erro na função corrige_veiculo_sem_combustivel_padrao {e}")

        if veiculo_sem_capacidade_volumetrica:
            dado = analisa_veiculo_sem_capacidade_volumetrica()

            if corrigirErros and len(dado) > 0:
                corrige_veiculo_sem_capacidade_volumetrica(listDados=dado)

    if dadosList:
        valida_corrige_veiculo_sem_combustivel_padrao(pre_validacao='veiculo_sem_combustivel_padrao')
        valida_corrige_veiculo_sem_capacidade_volumetrica(pre_validacao='veiculo_sem_capacidade_volumetrica')
