import logging

from ckan import plugins as p

log = logging.getLogger(__name__)


def harvest_source_delete(context, data_dict):
    '''Deletes an existing harvest source

    This method just proxies the request to package_delete,
    which will delete the actual harvest type dataset and the
    HarvestSource object (via the after_delete extension point).

    :param id: the name or id of the harvest source to delete
    :type id: string
    '''
    log.info('Deleting harvest source: %r', data_dict)

    p.toolkit.check_access('harvest_source_delete', context, data_dict)

    p.toolkit.get_action('package_delete')(context, data_dict)

    package_dict = p.toolkit.get_action('package_show')(context, data_dict)
    harvest_source_id = package_dict['id']

    source_delete = data_dict.get('source_delete')
    # source_dict = {'id': harvest_source_id, 'source_delete': source_delete}

    # print('################################ ckanext-harvest action.delete line 30 #################################')
    # print(context)
    if context.get('source_delete', False):
        harvest_source_rel_info_delete(context, {'id': harvest_source_id})

    # harvest_source_rel_info_delete(context, source_dict)
    if context.get('clear_source', False):
        # We need the id. The name won't work.

        p.toolkit.get_action('harvest_source_clear')(
            context, {'id': harvest_source_id})
        log.info('finish clear source')

    # # Add automatic harvest_source_job_history_clear
    #
    # # Refresh the index for this source to update the status object
    # p.toolkit.get_action('harvest_source_reindex')(context, {'id': harvest_source_id})

    # p.toolkit.get_action('harvest_source_job_history_clear')(
    #     context, {'id': package_dict['id']})

    # log.info('finish harvest source history clear')

    # model = context['model']
    # sql = '''begin;
    #     delete from harvest_source where id='{harvest_source_id}';
    #     commit;
    #     '''.format(harvest_source_id=package_dict['id'])
    # log.info('delete harvest source in harvest_source table')
    # model.Session.execute(sql)
    # log.info('finish delete harvest source in db')

def harvest_source_rel_info_delete(context, data_dict):

    harvest_source_id = data_dict['id']
    # source_delete = data_dict['source_delete']
    model = context['model']

    # harvest_source_index_clear

    sql = '''begin;
            update package set state = 'to_delete' where id in (
                select package_id from harvest_object
                where harvest_source_id = '{harvest_source_id}');'''.format(
        harvest_source_id=harvest_source_id)

    sql += '''
    delete from resource_view where resource_id in (
        select id from resource where package_id in (
            select id from package where state = 'to_delete'));
    delete from resource_revision where package_id in (
        select id from package where state = 'to_delete');
    delete from resource where package_id in (
        select id from package where state = 'to_delete');
        '''
    sql += '''
        delete from harvest_object_error where harvest_object_id in (
            select id from harvest_object
            where harvest_source_id = '{harvest_source_id}');
        delete from harvest_object_extra where harvest_object_id in (
            select id from harvest_object
            where harvest_source_id = '{harvest_source_id}');
        delete from harvest_object where harvest_source_id = '{harvest_source_id}';
        delete from harvest_gather_error where harvest_job_id in (
            select id from harvest_job where source_id = '{harvest_source_id}');
        delete from harvest_job where source_id = '{harvest_source_id}';
        delete from package_tag_revision where package_id in (
            select id from package where state = 'to_delete');
        delete from member_revision where table_id in (
            select id from package where state = 'to_delete');
        delete from package_extra_revision where package_id in (
            select id from package where state = 'to_delete');
        delete from package_revision where id in (
            select id from package where state = 'to_delete');
        delete from package_tag where package_id in (
            select id from package where state = 'to_delete');
        delete from package_extra where package_id in (
            select id from package where state = 'to_delete');
        delete from package_relationship_revision where subject_package_id in (
            select id from package where state = 'to_delete');
        delete from package_relationship_revision where object_package_id in (
            select id from package where state = 'to_delete');
        delete from package_relationship where subject_package_id in (
            select id from package where state = 'to_delete');
        delete from package_relationship where object_package_id in (
            select id from package where state = 'to_delete');
        delete from member where table_id in (
            select id from package where state = 'to_delete');
         '''.format(
        harvest_source_id=harvest_source_id)
    sql += '''
            delete from package where id in (
                select id from package where state = 'to_delete');
            '''

    # sql += '''begin;
    #         delete from harvest_object_error where harvest_object_id
    #          in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    #         delete from harvest_object_extra where harvest_object_id
    #          in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    #         delete from harvest_object where harvest_source_id = '{harvest_source_id}';
    #         delete from harvest_gather_error where harvest_job_id
    #          in (select id from harvest_job where source_id = '{harvest_source_id}');
    #         delete from harvest_job where source_id = '{harvest_source_id}';
    #         commit;
    #         '''.format(harvest_source_id=harvest_source_id)
    sql += '''
        commit;
        '''
    # model.Session.execute(sql)

    model.Session.execute(sql)

    # Refresh the index for this source to update the status object
    p.toolkit.get_action('harvest_source_reindex')(context, {'id': harvest_source_id})

    log.info('finish harvest source history clear')
    print('############################### check harvest_source_rel_info_delete data_dict line 93 ##################################')
    # harvest_source delete
    sql = '''begin;
            delete from harvest_source where id='{harvest_source_id}';
            commit;
            '''.format(harvest_source_id=harvest_source_id)
    log.info('delete harvest source in harvest_source table')
    model.Session.execute(sql)
    log.info('finish delete harvest source in db')

