from easycart_project.settings import generate_presigned_logo_url

def global_settings(request):
    return {
        "SIGNED_LOGO_URL": generate_presigned_logo_url()
    }

def product_categories(request):
    return {
        "categories": ["Phones", "Laptops", "Accessories"]
    }
