import __main__ as app
import json

from jinja2 import Template


class UsagePointSelect:

    def __init__(self, selected_usage_point=None, choice=False):
        self.application_path = app.APPLICATION_PATH
        self.selected_usage_point = selected_usage_point
        self.choice = choice

    def html(self):
        list_usage_points_id = '<select name="usages_points_id" id="select_usage_point_id" class="right">'
        if self.choice:
            list_usage_points_id += '<option value="none">--- Choix du point de livraison ---</option>'
        for config in app.DB.get_usage_point_all():
            usage_point = config.usage_point_id
            address_data = app.DB.get_addresse(usage_point)
            if hasattr(config, "name"):
                text = config.name
            elif address_data is not None:
                address_data = json.loads(address_data)
                street = address_data['usage_point']['usage_point_addresses']['street']
                postal_code = address_data['usage_point']['usage_point_addresses']['postal_code']
                city = address_data['usage_point']['usage_point_addresses']['city']
                country = address_data['usage_point']['usage_point_addresses']['country']
                text = f"{usage_point} - {street}, {postal_code} {city} {country}"
            else:
                text = usage_point
            select = ""
            if self.selected_usage_point == usage_point:
                select = "selected"
            list_usage_points_id += f'<option value="{usage_point}" {select}>{text}</option>'
        list_usage_points_id += '</select>'
        result = f'<h3 style="line-height: 45px; font-size: 25px;">Choix du point de livraison {list_usage_points_id}</h3>'
        if self.selected_usage_point is not None:
            result += f'<input type="text" id="usage_point_id" value="{self.selected_usage_point}" style="display: none">'
        return result

    def javascript(self):
        with open(f'{self.application_path}/templates/js/usage_point_select.js') as file_:
            side_menu = Template(file_.read())
        return side_menu.render()
