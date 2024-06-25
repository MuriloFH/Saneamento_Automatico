from validacoes.organograma import validaOrganograma
from validacoes.materiais import validaMateriais
from validacoes.prazoEntrega import validaPrazoEntrega
from validacoes.localEntrega import validaLocalEntrega
from validacoes.solicitacoes import validaSolicitacoesCompra
from validacoes.responsaveis import validaResponsaveis
from validacoes.ato import validaAtos
from validacoes.fornecedor import validaFornecedor
from validacoes.comissaoLicitacao import validaComissaoLicitacao
from validacoes.veiculoPublicacao import validaVeiculoPublicacao
from validacoes.processoAdm import validaProcessoAdm
from validacoes.ataRegistroPreco import validaAtaRegistroPreco
from validacoes.processoAdmItemLivre import validaProcessoAdmItemLivre
from validacoes.processoAdmItemReservado import validaProcessoAdmItemReservado
from validacoes.processoAdmLote import validaProcessoAdmLote
from validacoes.processoAdmConvidado import validaProcessoAdmConvidado
from validacoes.processoAdmImpugnacao import validaProcessoAdmImpugnacao
from validacoes.processoAdmParticipante import validaProcessoAdmParticipante
from validacoes.processoAdmParticipanteProposta import validaProcessoAdmParticipanteProposta
from validacoes.processoAdmInterposicaoRecurso import validaProcessoAdmInterposicaoRecurso
from validacoes.processoAdmAtoFinal import validaProcessoAdmAtoFinal
from validacoes.processoAdmAtoFinalRevogacao import validaProcessoAdmAtoFinalRevogacao
from validacoes.contratacoes import validaContratacoes
from validacoes.contratacoesItens import validaContratacoesItens
from validacoes.contratacaoAditivo import validaContratacaoAditivo
from validacoes.comprovante import validaComprovante
from validacoes.rescisaoContratual import validaRescisaoContratual
from validacoes.contratacoesAditivoItem import validaContratacaoAditivoItem
from validacoes.contratacoesApostila import validaContratacaoApostila
from validacoes.solicitacaoFornecimentoCompraDireta import validaSolicitacaoFornecimentoCompraDireta
from validacoes.solicitacaoFornecimento import validaSolicitacaoFornecimento
from validacoes.solicitacaoFornecimentoItem import validaSolicitacaoFornecimentoItem
from utilitarios.bancoDeAlteracoes.logAlteracoes import LogAlteracoes
from utilitarios.logExecucao.funcoesLogExecucao import LogTxt
from utilitarios.popularTabelaControle.popularTabelaControle import populaTabela
import datetime

logTxt = LogTxt()

nomeOdbc = ''

# ------------Inicia Banco de logs------------
logAlteracoes = LogAlteracoes(host="localhost",
                              dataBase="logsAlteracoes",
                              usuario="postgres",
                              senha="postgres")

logAlteracoes.criaTabela()
# --------------------------------------------

# ------------Popula a tabela de controle-----
populaTabela(odbc=nomeOdbc, sistema='Compras')
# --------------------------------------------
timeStart = datetime.datetime.now()
print(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"Inicio do saneamento > {timeStart.strftime('%H:%M:%S')}")
# ------------Inicia Pré-validações------------

validaContratacoes.contratacoes(tipo_registro='contratacoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                contrato_sem_itens=True)

validaOrganograma.organograma(tipo_registro='configuracoes-organogramas', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              orgao_nao_cadastrado=True,
                              unidade_nao_cadastrada=True,
                              centro_custo_nao_cadastrado=True)

validaMateriais.materiais(tipo_registro='materiais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                          corrigirErros=True,
                          materiais_tipo_servicos_nao_estocavel=True)

validaPrazoEntrega.prazosEntrega(tipo_registro='prazos-entrega', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                 corrigirErros=True,
                                 prazos_entrega_maior_quatro_digitos=True,
                                 descricao_prazos_entrega_maior_cinquenta_digitos=True)

validaLocalEntrega.locaisEntrega(tipo_registro='locais-entrega', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                 corrigirErros=True,
                                 local_entrega_nulo=True  # a correção será chamada independente se foi localizado erros na tabela de controle
                                 )

validaSolicitacoesCompra.solicitacoesCompra(tipo_registro='solicitacoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                            corrigirErros=True,
                                            solicitacao_compra_possui_data_diferente_do_exercicio=True
                                            )

validaResponsaveis.responsaveis(tipo_registro='responsaveis', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                corrigirErros=True,
                                responsavel_cpf_nulo_ou_invalido=True)

validaAtos.atos(tipo_registro='atos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                corrigirErros=True,
                ato_nulo=True,
                ato_data_designacao_ou_publicacao_nula=True)

validaFornecedor.fornecedores(tipo_registro='fornecedores', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              fornecedor_endereco_incompleto=True,
                              fornecedor_inscricao_municipal_duplicada=True,
                              fornecedor_inscricao_estadual_duplicada=True,
                              fornecedor_cpf_invalido=False,
                              fornecedor_mais_de_um_email=False)

validaComissaoLicitacao.comissoesLicitacao(tipo_registro='comissoes-licitacao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                           corrigirErros=True,
                                           cpf_autoridade_invalido=True,
                                           cpf_presidente_invalido=True,
                                           data_expiracao_nula=True,
                                           tipo_comissao_diferente_do_pregao=True,
                                           comissao_sem_data_criacao_ou_designacao=True,
                                           comissao_sem_responsavel_assinatura=True,
                                           tipo_comissao_diferente_leiloeiro=True,
                                           data_exoneracao_inferior_data_processo=True
                                           )

validaVeiculoPublicacao.veiculosPublicacao(tipo_registro='veiculos-publicacao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                           corrigirErros=True,
                                           processo_compra_nulo=True)

validaProcessoAdm.processoAdm(tipo_registro='processos-administrativo', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              licitacao_duplicada=True,
                              ano_contratacao_menor_ano_processo=True,
                              data_hora_abertura_licitacao_menor_inicio_recebimento=True,
                              data_hora_abertura_licitacao_menor_final_recebimento=True,
                              data_inicio_recebimento_menor_data_processo=True,
                              data_inicio_recebimento_menor_data_final_recebimento=True,
                              data_expiracao_comissao_menor_data_processo=True,
                              processo_ata_sem_data_homologacao=True,
                              processo_sem_participante_com_data_homologacao=True,
                              processo_homologado_adjudicado_sem_ata=True,
                              fundamento_processo_com_registro_preco_em_modalidade_especifica=True,
                              tipo_processo_nao_permite_registro_preco_informado=True,
                              nome_orgao_processo_maior_60_caracter=True,
                              processo_homologado_sem_ata=True,
                              modalidade_processo_outras_com_tipo_objeto_diferente_5=True,
                              quantidade_item_superior_licitada=True,
                              incisao_nulo_processos_tipo_pregao=True,
                              dispensa_pregao_com_inciso_incorreto=True,
                              registro_preco_nulo_com_contrato_marcado_ata_registro_preco=True,
                              processo_com_movimentacoes_com_coluna_data_contrato_transformers_nulo=True
                              )

validaProcessoAdm.processoAdm(tipo_registro='processos-administrativo-item-entidades', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              saldo_pendente_entre_entidades=True, )

validaProcessoAdmItemLivre.processoAdmItemLivre(tipo_registro='processos-administrativo-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                processo_so_mpes_sem_configuracao_favorecimento_mpes=True)

validaProcessoAdmItemReservado.processoAdmItemReservado(tipo_registro='processos-administrativo-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                        corrigirErros=True,
                                                        item_cota_reservada_processo_configuracao_exclusividade=True)

validaProcessoAdmLote.processoAdmLote(tipo_registro='processos-administrativo-lotes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                      corrigirErros=True,
                                      lote_processo_adm_duplicidade_descricao=True)

validaProcessoAdmConvidado.processoAdmConvidado(tipo_registro='convidado-licitacao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                corrigirErros=True,
                                                data_protocolo_convidado_nula=True)

validaProcessoAdmImpugnacao.processoAdmImpugnacao(tipo_registro='processo-administrativo-impugnacoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                  corrigirErros=True,
                                                  data_julgamento_impugnacao_nula=True)

validaProcessoAdmParticipante.processoAdmParticipante(tipo_registro='participante-licitacao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                      corrigirErros=True,
                                                      participante_nao_convidado_para_processo=True)

validaProcessoAdmParticipanteProposta.processoAdmParticipanteProposta(tipo_registro='proposta-participante', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                                      corrigirErros=True,
                                                                      situacao_proposta_incorreta_para_processos_homologados=True,
                                                                      credor_com_itens_sem_proposta=True,
                                                                      proposta_participante_classificacao_invalida=True,
                                                                      divergencia_quantidade_participantes_com_e_sem_proposta=True)

validaProcessoAdmInterposicaoRecurso.processoAdmInterposicaoRecurso(tipo_registro='processo-administrativo-interposicoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                                    corrigirErros=True,
                                                                    situacao_interposicao_recurso_invalida=True)

validaProcessoAdmAtoFinal.processoAdmAtoFinal(tipo_registro='processo-administrativo-atos-finais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              data_anulacao_revogacao_processo_anterior_data_homologacao=True,
                                              responsavel_anulacao_nao_informado=True,
                                              processo_homologado_sem_participante_vencedor=True)

validaProcessoAdmAtoFinalRevogacao.processoAdmAtoFinalRevogacao(tipo_registro='processo-administrativo-atos-finais', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                                corrigirErros=True,
                                                                responsavel_revogacao_nao_informado=True)

validaAtaRegistroPreco.ataRegistroPreco(tipo_registro='atas-registro-preco', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                        corrigirErros=True,
                                        ata_registro_preco_sem_processo_homologado=True,
                                        ano_data_ata_diferente_ano_data_registro_preco=True,
                                        fornecedor_ata_diferente_fornecedor_vencedor_processo=True)

validaProcessoAdmParticipante.processoAdmParticipante(tipo_registro='participante-licitacao', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                      corrigirErros=True,
                                                      representante_participante_cpf_invalido=True)

validaContratacaoAditivo.contratacaoAditivo(tipo_registro='contratacoes-aditivos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                            corrigirErros=True,
                                            data_aditivo_fora_periodo_vigencia_contrato=True,
                                            vigencia_aditivo_nulo=True,
                                            vigencia_final_aditivo_diferente_dt_contrato_principal=True)

validaContratacaoAditivoItem.contratacaoAditivoItem(tipo_registro='aditivos-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                    corrigirErros=True,
                                                    aditivo_sem_item=True,
                                                    aditivo_de_prazo_com_itens=True)

validaContratacaoApostila.contratacaoApostila(tipo_registro='contratacoes-apostilamentos', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                              corrigirErros=True,
                                              data_historico_fora_vigencia_contrato=True)

validaSolicitacaoFornecimentoCompraDireta.solicitacaoFornecimentoCompraDireta(tipo_registro='solicitacoes-fornecimento', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                                              corrigirErros=True,
                                                                              credor_af_diferente_da_compra_direta=True)

validaSolicitacaoFornecimento.solicitacaoFornecimento(tipo_registro='solicitacoes-fornecimento', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                      corrigirErros=True,
                                                      data_sf_menor_data_inicio_contrato=True,
                                                      data_sf_menor_data_fim_contrato=True,
                                                      data_homologacao_processo_maior_data_sf=True,
                                                      sf_sem_contrato_informado=True)

validaSolicitacaoFornecimentoItem.solicitacaoFornecimentoItem(tipo_registro='solicitacao-fornecimento-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                                              corrigirErros=True,
                                                              item_nao_presente_no_contrato=True,
                                                              qtd_solicitacao_item_maior_qtd_consumo_previsto_no_contrato=True)

validaContratacoesItens.contratacoesItens(tipo_registro='contratacoes-itens', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                          corrigirErros=True,
                                          saldo_item_diferente_distribuicao_saldo_entre_entidades=True,
                                          item_vinculado_a_multiplos_contratos_com_quantidade_maior_que_processo=True)

validaComprovante.comprovante(tipo_registro='comprovantes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                              corrigirErros=True,
                              liquidacao_af_documento_zerado=True)

validaRescisaoContratual.rescisaoContratual(tipo_registro='contratacoes-rescisoes', nomeOdbc=nomeOdbc, logAlteracoes=logAlteracoes, logSistema=logTxt,
                                            corrigirErros=True,
                                            rescisao_contrato_vencido=True)

# ------------Finaliza Pré-validações----------

timeEnd = datetime.datetime.now()
print(f"\nFim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.escreveLog(f"\nFim do saneamento > {timeEnd.strftime('%H:%M:%S')}")
logTxt.fechaArquivo()

print(f"Tempo de execução: {timeEnd - timeStart}")
