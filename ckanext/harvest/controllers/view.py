import re
import xml.etree.ElementTree as etree

try:
    # Python 2.7
    xml_parser_exception = etree.ParseError
except AttributeError:
    # Python 2.6
    from xml.parsers import expat

    xml_parser_exception = expat.ExpatError

from pylons.i18n import _

from ckan import model

import ckan.plugins as p
import ckan.lib.helpers as h, json
from ckan.lib.base import BaseController, c, request, response, render, abort

from ckanext.harvest.logic import HarvestJobExists, HarvestSourceInactiveError
from ckanext.harvest.plugin import DATASET_TYPE_NAME

import logging

log = logging.getLogger(__name__)


class ViewController(BaseController):

    not_auth_message = p.toolkit._('Not authorized to see this page')

    def __before__(self, action, **params):

        super(ViewController, self).__before__(action, **params)

        c.dataset_type = DATASET_TYPE_NAME

    def delete(self, id):
        try:
            context = {'model': model, 'user': c.user}

            context['clear_source'] = request.params.get('clear', '').lower() in (
                u'true',
                u'1',
            )
            context['source_delete'] = request.params.get('source_delete', '').lower() in (
                u'true',
                u'1',
            )
            p.toolkit.get_action('harvest_source_delete')(context, {'id': id})
            if context['clear_source']:
                h.flash_success(_('Harvesting source successfully cleared'))
            elif context['source_delete']:
                h.flash_success(_('Harvesting source successfully deleted'))
            else:
                h.flash_success(_('Harvesting source successfully inactivated'))

            h.redirect_to(h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=id))
        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)

    def refresh(self, id):
        try:
            context = {'model': model, 'user': c.user, 'session': model.Session}
            p.toolkit.get_action('harvest_job_create')(
                context, {'source_id': id, 'run': True}
            )
            h.flash_success(
                _('Harvest will start shortly. Refresh this page for updates.')
            )
        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except HarvestSourceInactiveError:
            h.flash_error(
                _(
                    'Cannot create new harvest jobs on inactive '
                    'sources. First, please change the source status '
                    'to "active".'
                )
            )
        except HarvestJobExists:
            h.flash_notice(
                _('A harvest job has already been scheduled for ' 'this source')
            )
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            h.flash_error(msg)

        h.redirect_to(h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=id))

    def clear(self, id):
        try:
            context = {'model': model, 'user': c.user, 'session': model.Session}
            p.toolkit.get_action('harvest_source_clear')(context, {'id': id})
            h.flash_success(_('Harvest source cleared'))
        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            h.flash_error(msg)

        h.redirect_to(h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=id))

    def show_object(self, id, ref_type='object'):

        try:
            context = {'model': model, 'user': c.user}
            if ref_type == 'object':
                obj = p.toolkit.get_action('harvest_object_show')(context, {'id': id})
            elif ref_type == 'dataset':
                obj = p.toolkit.get_action('harvest_object_show')(
                    context, {'dataset_id': id}
                )

            # Check content type. It will probably be either XML or JSON
            try:

                if obj['content']:
                    content = obj['content']
                elif 'original_document' in obj['extras']:
                    content = obj['extras']['original_document']
                else:
                    abort(404, _('No content found'))
                try:
                    etree.fromstring(re.sub('<\?xml(.*)\?>', '', content))
                except UnicodeEncodeError:
                    etree.fromstring(
                        re.sub('<\?xml(.*)\?>', '', content.encode('utf-8'))
                    )
                response.content_type = 'application/xml; charset=utf-8'
                if not '<?xml' in content.split('\n')[0]:
                    content = u'<?xml version="1.0" encoding="UTF-8"?>\n' + content

            except xml_parser_exception:
                try:
                    json.loads(obj['content'])
                    response.content_type = 'application/json; charset=utf-8'
                except ValueError:
                    # Just return whatever it is
                    pass

            response.headers['Content-Length'] = len(content)
            return content.encode('utf-8')
        except p.toolkit.ObjectNotFound as e:
            abort(404, _(str(e)))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500, msg)

    def _get_source_for_job(self, source_id):

        try:
            context = {'model': model, 'user': c.user}
            source_dict = p.toolkit.get_action('harvest_source_show')(
                context, {'id': source_id}
            )
        except p.toolkit.ObjectNotFound:
            abort(404, p.toolkit._('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500, msg)

        return source_dict

    def show_job(self, id, source_dict=False, is_last=False):

        try:
            context = {'model': model, 'user': c.user}
            job = p.toolkit.get_action('harvest_job_show')(context, {'id': id})
            job_report = p.toolkit.get_action('harvest_job_report')(
                context, {'id': id}
            )

            if not source_dict:
                source_dict = p.toolkit.get_action('harvest_source_show')(
                    context, {'id': job['source_id']}
                )

            return render(
                'source/job/read.html',
                extra_vars={
                    'harvest_source': source_dict,
                    'job': job,
                    'job_report': job_report,
                    'is_last_job': is_last,
                },
            )

        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest job not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500, msg)

    def about(self, id):
        try:
            context = {'model': model, 'user': c.user}
            harvest_source = p.toolkit.get_action('harvest_source_show')(
                context, {'id': id}
            )
            return render(
                'source/about.html', extra_vars={'harvest_source': harvest_source}
            )
        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)

    def admin(self, id):
        try:
            context = {'model': model, 'user': c.user}
            p.toolkit.check_access('harvest_source_update', context, {'id': id})
            harvest_source = p.toolkit.get_action('harvest_source_show')(
                context, {'id': id}
            )
            return render(
                'source/admin.html', extra_vars={'harvest_source': harvest_source}
            )
        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)

    def abort_job(self, source, id):
        try:
            h.flash_success(_('Harvest job stopped'))

        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest job not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500, msg)

        h.redirect_to(h.url_for('{0}_admin'.format(DATASET_TYPE_NAME), id=source))

    def show_last_job(self, source):

        source_dict = self._get_source_for_job(source)

        if not source_dict['status']['last_job']:
            abort(404, _('No jobs yet for this source'))

        return self.show_job(
            source_dict['status']['last_job']['id'],
            source_dict=source_dict,
            is_last=True,
        )

    def list_jobs(self, source):

        try:
            context = {'model': model, 'user': c.user}
            harvest_source = p.toolkit.get_action('harvest_source_show')(
                context, {'id': source}
            )
            jobs = p.toolkit.get_action('harvest_job_list')(
                context, {'source_id': harvest_source['id']}
            )

            return render(
                'source/job/list.html',
                extra_vars={'harvest_source': harvest_source, 'jobs': jobs},
            )

        except p.toolkit.ObjectNotFound:
            abort(404, _('Harvest source not found'))
        except p.toolkit.NotAuthorized:
            abort(401, self.not_auth_message)
        except Exception as e:
            msg = 'An error occurred: [%s]' % str(e)
            abort(500, msg)
