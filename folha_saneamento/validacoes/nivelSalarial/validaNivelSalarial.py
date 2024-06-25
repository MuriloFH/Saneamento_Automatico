from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def nivelSalarial(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                  corrigirErros=False,
                  niveis_salariais_com_descricao_duplicada=False
                  ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_niveis_salariais_com_descricao_duplicada(pre_validacao):
        nomeValidacao = "Nivel salarial com descrição duplicada."

        def analisa_niveis_salariais_com_descricao_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_niveis_salariais_com_descricao_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    select 
                        i_entidades,
                        nome,
                        count(nome) as quantidade,
                        list(i_niveis) as codigos
                    from bethadba.niveis
                    group by i_entidades, nome
                    having count(nome) > 1   
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_niveis = row['codigos'].split(',')
                        for nivel in list_niveis[1:]:
                            novo_nome = row['nome'] + ' ' + nivel
                            if len(novo_nome) <= 50:
                                dadoAlterado.append(f"Alterado nome do nivel salarial {nivel} entidade {row['i_entidades']} de {row['nome']} para {novo_nome}")
                                comandoUpdate += f"""UPDATE bethadba.niveis set nome = '{novo_nome}' where i_entidades = {row['i_entidades']} and i_niveis = {nivel};\n"""
                            else:
                                novo_nome = novo_nome[:50 - len(nivel)] + nivel
                                dadoAlterado.append(f"Alterado nome do nivel salarial {nivel} entidade {row['i_entidades']} de {row['nome']} para {novo_nome}")
                                comandoUpdate += f"""UPDATE bethadba.niveis set nome = '{novo_nome}' where i_entidades = {row['i_entidades']} and i_niveis = {nivel};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_niveis_salariais_com_descricao_duplicada: {e}")
            return

        if niveis_salariais_com_descricao_duplicada:
            dado = analisa_niveis_salariais_com_descricao_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_niveis_salariais_com_descricao_duplicada(listDados=dado)

    if dadosList:
        analisa_corrige_niveis_salariais_com_descricao_duplicada(pre_validacao="niveis_salariais_com_descricao_duplicada")
