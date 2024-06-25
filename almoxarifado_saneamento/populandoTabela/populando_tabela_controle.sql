-- unidade de medida
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'unidade_medida_sem_abreviatura_ou_nome'
where tipo_registro = 'unidades-medida'
and mensagem_erro like '%não possui abreviatura ou nome informado%'


-- materiais descrição duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'materiais_descricao_duplicada'
where tipo_registro = 'materiais'
and mensagem_erro like '%possui a mesma descrição%'

--lotes-materiais possui a data de fabricação maior que a data de validade
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_fabricacao_maior_que_data_de_validade'
where tipo_registro = 'lotes-materiais'
and mensagem_erro like '%possui a data de fabricação maior que a data de validade%'

--lotes_duplicados
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'lotes_duplicados'
where tipo_registro = 'lotes-materiais'
and mensagem_erro like '%possui o mesmo nº de lote, material, data de fabricação e data de validade%'


--cnpj_cpf_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_cpf_invalido'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%inválido%'

--fornecedor_estado_nulo
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'fornecedor_estado_nulo'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%não possui a cidade ou o estado válido informado%'

--cnpj_cpf_duplicado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'cnpj_cpf_duplicado'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%possui o mesmo CPF/CNPJ do(s) fornecedor(es)%'

--inscricao_estadual_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'inscricao_estadual_duplicada'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%possui a mesma Inscrição Estadual%'

--inscricao_municipal_duplicada
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'inscricao_municipal_duplicada'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%possui a mesma Inscrição Municipal%'

--data_situacao_maior_data_atual
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'data_situacao_maior_data_atual'
where tipo_registro = 'fornecedores'
and mensagem_erro like '%possui a Data Situação maior que a data atual%'

--mascara_incorreta_nivel_zerado
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'mascara_incorreta_nivel_zerado'
where tipo_registro = 'organogramas'
and mensagem_erro like '%do Betha Estoque possui sua máscara incorreta, ou a máscara do centro de custo é do 3º nível, com o 2º nível zerado%'

--responsavel_cpf_nulo_ou_invalido
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'responsavel_cpf_nulo_ou_invalido'
where tipo_registro = 'responsaveis'
and mensagem_erro like '%não possui CPF informado ou é invalido%'

--saida_menor_que_a_soma_das_entradas
update bethadba.controle_migracao_registro_ocor
set pre_validacao = 'saida_menor_que_a_soma_das_entradas'
where tipo_registro = 'saidas-itens'
and mensagem_erro like '%menor que a soma das quantidades de saídas%'

commit;

