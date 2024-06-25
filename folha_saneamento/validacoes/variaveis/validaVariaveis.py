from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta


def variavel(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
             corrigirErros=False,
             variaveis_dt_inicial_ou_final_maior_dt_rescisao=False,
             considera_conselheiro=False
             ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_variaveis_dt_inicial_ou_final_maior_dt_rescisao(pre_validacao):
        nomeValidacao = "Históricos salariais com salário zerado ou nulo"

        def analisa_variaveis_dt_inicial_ou_final_maior_dt_rescisao():
            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_variaveis_dt_inicial_ou_final_maior_dt_rescisao(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoDelete = ""
            comandoUpdate = ""
            dadoAlterado = []
            dadoDeletado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                query = """
                        select 
                            *, 
                            left(aux.data_afastamento,8) || '01' as nova_data_final
                        from (
                            select a.i_entidades , a.i_funcionarios, COALESCE(ta.classif,1) as classif , max(a.dt_afastamento) as data_afastamento, f.conselheiro_tutelar , v.i_eventos, v.i_processamentos, v.i_tipos_proc , v.dt_inicial , v.dt_final 
                            from bethadba.afastamentos a
                            join bethadba.tipos_afast ta on (ta.i_tipos_afast = a.i_tipos_afast)
                            join bethadba.funcionarios f on (f.i_entidades = a.i_entidades and f.i_funcionarios = a.i_funcionarios)
                            left join bethadba.variaveis v on (v.i_entidades = a.i_entidades and v.i_funcionarios = a.i_funcionarios)
                            where ta.classif in (8)
                            and a.dt_afastamento  <= today()
                            and isnull(a.dt_ultimo_dia,today()) >= today()
                            and v.i_eventos is not null
                            and f.conselheiro_tutelar = 'N'
                            group by a.i_entidades, a.i_funcionarios, ta.classif, f.conselheiro_tutelar, v.i_eventos, v.i_processamentos, v.i_tipos_proc , v.dt_inicial , v.dt_final 
                            order by a.i_entidades, a.i_funcionarios,  v.dt_inicial , v.dt_final )aux
                        where aux.dt_inicial > aux.data_afastamento OR aux.dt_final > aux.data_afastamento  
                 """
                if not considera_conselheiro:
                    busca = banco.consultar(query)
                else:
                    query = query.replace("and f.conselheiro_tutelar = 'N'","")
                    busca = banco.consultar(query)

                if len(busca) > 0:
                    for row in busca:

                        # Na tratativa quando a data inicial do lançamento da variavel for maior que a data da rescisão o lançamento da variável será deletado, caso contrário serã alterada data final do lançamento para ser igual a data da rescisao.
                        if row['dt_inicial'] > row['data_afastamento']:
                            dadoDeletado.append(f"Deletado lançamento da variável {row['i_eventos']} entidade: {row['i_entidades']}, Funcionario: {row['i_funcionarios']}, processamentos: {row['i_processamentos']}, tipo de processamento: {row['i_tipos_proc']} com data inicial {row['dt_inicial']} e data final {row['dt_final']}")
                            comandoDelete += f"""DELETE FROM bethadba.variaveis_emprestimos_parc WHERE i_entidades = {row['i_entidades']} AND i_funcionarios = {row['i_funcionarios']} AND i_eventos = {row['i_eventos']} AND i_processamentos = {row['i_processamentos']} AND i_tipos_proc = {row['i_tipos_proc']} AND dt_inicial = '{row['dt_inicial']}' AND dt_final = '{row['dt_final']}';\n"""
                            comandoDelete += f"""DELETE FROM bethadba.variaveis WHERE i_entidades = {row['i_entidades']} AND i_funcionarios = {row['i_funcionarios']} AND i_eventos = {row['i_eventos']} AND i_processamentos = {row['i_processamentos']} AND i_tipos_proc = {row['i_tipos_proc']} AND dt_inicial = '{row['dt_inicial']}' AND dt_final = '{row['dt_final']}';\n"""
                        else:
                            dadoAlterado.append(f"Alterado data final do lançamento da variável {row['i_eventos']} entidade: {row['i_entidades']}, Funcionario: {row['i_funcionarios']}, processamentos: {row['i_processamentos']}, tipo de processamento: {row['i_tipos_proc']} com data inicial {row['dt_inicial']} para {row['nova_data_final']}")
                            comandoUpdate += f"""UPDATE bethadba.variaveis_emprestimos_parc set dt_final = '{row['nova_data_final']}' WHERE i_entidades = {row['i_entidades']} AND i_funcionarios = {row['i_funcionarios']} AND i_eventos = {row['i_eventos']} AND i_processamentos = {row['i_processamentos']} AND i_tipos_proc = {row['i_tipos_proc']} AND dt_inicial = '{row['dt_inicial']}' AND dt_final = '{row['dt_final']}';\n"""
                            comandoUpdate += f"""UPDATE bethadba.variaveis set dt_final = '{row['nova_data_final']}' WHERE i_entidades = {row['i_entidades']} AND i_funcionarios = {row['i_funcionarios']} AND i_eventos = {row['i_eventos']} AND i_processamentos = {row['i_processamentos']} AND i_tipos_proc = {row['i_tipos_proc']} AND dt_inicial = '{row['dt_inicial']}' AND dt_final = '{row['dt_final']}';\n"""


                banco.executarComLog(comando=banco.triggerOff(comandoDelete, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao="EXCLUSÃO", nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoDeletado)

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate, folha=True), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Folha",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_variaveis_dt_inicial_ou_final_maior_dt_rescisao: {e}")
            return

        if variaveis_dt_inicial_ou_final_maior_dt_rescisao:
            dado = analisa_variaveis_dt_inicial_ou_final_maior_dt_rescisao()

            if corrigirErros and len(dado) > 0:
                corrige_variaveis_dt_inicial_ou_final_maior_dt_rescisao(listDados=dado)

    if dadosList:
        analisa_corrige_variaveis_dt_inicial_ou_final_maior_dt_rescisao(pre_validacao="variaveis_dt_inicial_ou_final_maior_dt_rescisao")
