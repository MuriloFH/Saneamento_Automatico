from utilitarios.coletaDados import tabelaControle
from utilitarios.conexao.conectaOdbc import Conecta
import polars as pl


def saidasItens(tipo_registro, nomeOdbc, logAlteracoes=None, logSistema=None,
                corrigirErros=False,
                codigo_material_duplicado=False,
                saida_menor_que_a_soma_das_entradas=False
                ):
    print('-' * 100)
    logSistema.escreveLog('-' * 100)

    banco = Conecta(odbc=nomeOdbc)
    dadosList = tabelaControle.buscaDados(tipo_registro=tipo_registro, odbc=nomeOdbc, logSistema=logSistema)

    def analisa_corrige_codigo_material_duplicado(pre_validacao):
        nomeValidacao = "Saídas Itens com material em duplicidade"

        def analisa_codigo_material_duplicado():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = banco.consultar("""
                select
                    m.i_saida,
                    count(*),
                    m.i_material,
                    sum(m.quantidade) as quantidade,
                    sum(m.vlrtotal) as vlrtotal,
                    max(m.item) as maior_item,
                    max(m.i_classe)  as i_classe,
                    m.i_entidades 
                from bethadba.movimentos m 
                where m.i_saida is not null
                group by m.i_saida,m.i_material, m.i_entidades 
                having count(*) > 1
                order by 1
            """)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_codigo_material_duplicado(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoInsert = ""
            comandoDelete = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""
                    select
                        m.i_saida,
                        count(*),
                        m.i_material,
                        sum(m.quantidade) as quantidade,
                        sum(m.vlrtotal) as vlrtotal,
                        max(m.item) as maior_item,
                        max(m.i_classe)  as i_classe,
                        m.i_entidades 
                    from bethadba.movimentos m 
                    where m.i_saida is not null
                    group by m.i_saida,m.i_material, m.i_entidades 
                    having count(*) > 1
                    order by 1 
                """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    for row in df.iter_rows(named=True):
                        dadoAlterado.append(f"Agrupado o material {row['i_material']} da saída {row['i_saida']}")
                        # delete
                        comandoDelete += f"delete bethadba.movimentos where i_saida = {row['i_saida']} and i_material = {row['i_material']} and i_entidades = {row['i_entidades']};\n"
                        # insert
                        comandoInsert += f"insert into bethadba.movimentos(i_saida,i_classe,i_material,quantidade,vlrtotal,estornado,item,i_entidades) values({row['i_saida']},{row['i_classe']},{row['i_material']},{row['quantidade']},{row['vlrtotal']},'N',{row['maior_item']},{row['i_entidades']});\n"

                # delete
                banco.executar(comando=banco.triggerOff(comandoDelete))
                # insert
                banco.executarComLog(comando=banco.triggerOff(comandoInsert), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_codigo_material_duplicado: {e}")
            return

        if codigo_material_duplicado:
            dado = analisa_codigo_material_duplicado()

            if corrigirErros and len(dado) > 0:
                corrige_codigo_material_duplicado(listDados=dado)

    def analisa_corrige_saida_menor_que_a_soma_das_entradas(pre_validacao):
        nomeValidacao = "Até a data da saída, o material possui a SOMA das quantidades de entradas menor que a soma das quantidades de saídas, tornando o saldo físico do material negativo"

        def analisa_saida_menor_que_a_soma_das_entradas():

            print(f">> Iniciando a validação '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a validação '{nomeValidacao}' ")

            dados = []
            for i in dadosList:
                if i['pre_validacao'] == f'{pre_validacao}':
                    dados.append(i)

            print(f">> Total de inconsistências encontradas: {len(dados)}")
            logSistema.escreveLog(f">> Total de inconsistências encontradas: {len(dados)}")

            return dados

        def corrige_saida_menor_que_a_soma_das_entradas(listDados):
            tipoCorrecao = "ALTERACAO"
            comandoUpdate = ""
            dadoAlterado = []

            print(f">> Iniciando a correção '{nomeValidacao}' ")
            logSistema.escreveLog(f">> Iniciando a correção '{nomeValidacao}'")

            try:
                busca = banco.consultar("""select 
                                                m.i_entidades,
                                                m.i_saida,
                                                s.i_estoque,
                                                m.item,
                                                s.datasaida,
                                                m.i_material,
                                                m.vlrtotal,
                                                m.quantidade as quantidadeItem,
                                                coalesce((select SUM(maux.quantidade) as totalQuantidadeEntrada from bethadba.movimentos maux
                                                        join bethadba.entradas eaux on (eaux.i_entrada = maux.i_entrada and eaux.i_entidades = maux.i_entidades)
                                                        where maux.estornado = 'N'
                                                            and maux.i_classe = m.i_classe
                                                            and maux.i_material = m.i_material
                                                            and eaux.i_estoque = s.i_estoque
                                                            and eaux.dataentrada <= s.datasaida
                                                            and eaux.dataentrada >= (select min(ee.dataentrada) from bethadba.entradas ee )), 0) as quantidadeEntrada,	-- Alterar Data
                                                coalesce((select coalesce(SUM(maux.quantidade),0)  as totalQuantidadeSaida from bethadba.movimentos maux
                                                        join bethadba.saidas saux on (saux.i_saida = maux.i_saida and saux.i_entidades = maux.i_entidades)
                                                        where maux.estornado = 'N'
                                                            and maux.i_classe = m.i_classe
                                                            and maux.i_material = m.i_material
                                                            and saux.i_estoque = s.i_estoque
                                                            and saux.datasaida <= s.datasaida
                                                            and saux.datasaida >= (select min(ss.datasaida) from bethadba.saidas ss)),0) as quantidadeSaida,	-- Alterar Data
                                                            (quantidadeEntrada - quantidadeSaida) as saldo
                                            from bethadba.movimentos m
                                            join bethadba.saidas s on (s.i_saida = m.i_saida and s.i_entidades = m.i_entidades)
                                            where saldo < 0
                                            order by s.datasaida 
                                            """)

                if len(busca) > 0:
                    df = pl.DataFrame(busca)
                    df = df.group_by(['i_material', 'i_estoque', 'i_entidades']).map_groups(
                        lambda df: df.with_columns(
                            (pl.col('saldo') - pl.col('saldo').shift()).fill_null(pl.col('saldo')).alias('saldo_sem_acumular')
                        )
                    )
                    for row in df.iter_rows(named=True):
                        saldo = row['quantidadeItem'] + row['saldo_sem_acumular']
                        novo_vlrtotal = saldo * (row['vlrtotal'] / row['quantidadeItem'])
                        dadoAlterado.append(f"Alterado a quantidade de saida do material {row['i_material']} da saida {row['i_saida']} do estoque {row['i_estoque']} da entidade {row['i_entidades']} com data em {row['datasaida']} para {saldo}")
                        comandoUpdate += f"""UPDATE bethadba.movimentos set quantidade = {saldo}, vlrtotal = '{novo_vlrtotal}' where i_saida = {row['i_saida']} and i_material = {row['i_material']} and i_entidades = {row['i_entidades']};\n"""

                banco.executarComLog(comando=banco.triggerOff(comandoUpdate), logAlteracoes=logAlteracoes, tipoCorrecao=tipoCorrecao, nomeOdbc=nomeOdbc, sistema="Estoque",
                                     tipoValidacao=tipo_registro, preValidacaoBanco=pre_validacao, dadoAlterado=dadoAlterado)

                print(f">> Finalizado a correção '{nomeValidacao}'")
                logSistema.escreveLog(f">> Finalizado a correção '{nomeValidacao}'")
            except Exception as e:
                print(f"Erro na função corrige_saida_menor_que_a_soma_das_entradas: {e}")
            return

        if saida_menor_que_a_soma_das_entradas:
            dado = analisa_saida_menor_que_a_soma_das_entradas()

            if corrigirErros and len(dado) > 0:
                corrige_saida_menor_que_a_soma_das_entradas(listDados=dado)

    # Não possui pré validação no Arqjob
    analisa_corrige_codigo_material_duplicado(pre_validacao="codigo_material_duplicado")

    if dadosList:
        analisa_corrige_saida_menor_que_a_soma_das_entradas(pre_validacao="saida_menor_que_a_soma_das_entradas")
