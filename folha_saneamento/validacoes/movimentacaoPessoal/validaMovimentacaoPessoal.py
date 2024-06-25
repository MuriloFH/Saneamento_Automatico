from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def movimentacaoPessoal(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                        corrigirErros=False,
                        movimentacao_pessoal_duplicada=False,
                        dt_vigorar_ato_maior_dt_movimentacao=False
                        ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_movimentacao_pessoal_duplicada(pre_validacao):
        nomeValidacao = "Movimentações de pessoal com descrição duplicada"

        def analisa_movimentacao_pessoal_duplicada():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_movimentacao_pessoal_duplicada(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar(f"""
                    SELECT 
                        list(i_tipos_movpes) as tiposs, 
                        descricao,
                        count(descricao) AS quantidade 
                    FROM 
                        bethadba.tipos_movpes 
                    GROUP BY 
                        descricao 
                    HAVING
                        quantidade > 1  
                 """)

                if len(busca) > 0:
                    for row in busca:
                        list_movto = row['tiposs'].split(',')
                        for mvto in list_movto[1:]:
                            nova_descricao = row['descricao'] + ' ' + mvto
                            # Tratativa leva em consideração o tamanho maximo do campo no Sybase que é 50.
                            if len(nova_descricao) <= 50:
                                dadoAlterado.append(f"Alterada descrição da movimentação de pessoal {mvto} de  {row['descricao']} para {nova_descricao}")
                                comandoUpdate += f"""UPDATE bethadba.tipos_movpes set descricao = '{nova_descricao}' where i_tipos_movpes = {mvto};\n"""
                            else:
                                sub = row['descricao'][:50 - len(mvto)] + mvto
                                dadoAlterado.append(f"Alterada descrição da movimentação de pessoal {mvto} de  {row['descricao']} para {sub}")
                                comandoUpdate += f"""UPDATE bethadba.tipos_movpes set descricao = '{sub}' where i_tipos_movpes = {mvto};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_movimentacao_pessoal_duplicada: {e}")
            return

        if movimentacao_pessoal_duplicada:
            dado = analisa_movimentacao_pessoal_duplicada()

            if corrigirErros and len(dado) > 0:
                corrige_movimentacao_pessoal_duplicada(listDados=dado)

    def analisa_corrige_dt_vigorar_ato_maior_dt_movimentacao(pre_validacao):
        nomeValidacao = "Data a vigorar do ato maior que a movimentação"

        def analisa_dt_vigorar_ato_maior_dt_movimentacao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_dt_vigorar_ato_maior_dt_movimentacao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                dados = banco.consultar(f"""               
                                            select
                                                af.i_entidades,
                                                af.i_funcionarios,
                                                af.i_atos_func,
                                                a.i_atos,
                                                a.dt_vigorar,
                                                af.dt_movimento
                                            from
                                                bethadba.atos_func af
                                                left join bethadba.atos a on
                                                af.i_atos = a.i_atos
                                            where a.dt_vigorar > af.dt_movimento
                                            order by af.i_entidades, af.i_funcionarios, af.i_atos_func
                                        """)
                for row in dados:
                    newDtMovimento = row['dt_vigorar']

                    dadoAlterado.append(f"Alterado a data de movimento para {newDtMovimento} do ato {row['i_atos_func']} do funcionário {row['i_funcionarios']} da entidade {row['i_entidades']}")
                    comandoUpdate += f"""UPDATE bethadba.atos_func set dt_movimento = '{newDtMovimento}' where i_entidades = {row['i_entidades']} and i_funcionarios = {row['i_funcionarios']} and i_atos_func = {row['i_atos_func']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_dt_vigorar_ato_maior_dt_movimentacao: {e}")
            return

        if dt_vigorar_ato_maior_dt_movimentacao:
            dado = analisa_dt_vigorar_ato_maior_dt_movimentacao()

            if corrigirErros and len(dado) > 0:
                corrige_dt_vigorar_ato_maior_dt_movimentacao(listDados=dado)

    if dadosList:
        analisa_corrige_movimentacao_pessoal_duplicada(pre_validacao="movimentacao_pessoal_duplicada")
        analisa_corrige_dt_vigorar_ato_maior_dt_movimentacao(pre_validacao="dt_vigorar_ato_maior_dt_movimentacao")
