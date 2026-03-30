import boto3
from django.shortcuts import render, redirect
from django.conf import settings
from django.contrib import messages
from django.contrib.messages import get_messages
from botocore.exceptions import ClientError
from easycart_rate_limiter import check_rate_limit
from botocore.config import Config
import hmac
import hashlib
import base64
import requests
from django.http import JsonResponse


# =========================
# Home / Base
# =========================
def base(request):
    return render(request, 'base.html')


def home(request):
    return render(request, 'home.html')


# =========================
# Helpers
# =========================
def get_cognito_client():
    return boto3.client(
        "cognito-idp",
        region_name=settings.COGNITO["region"],
    )


def clear_messages(request):
    storage = get_messages(request)
    for _ in storage:
        pass


def get_secret_hash(username: str) -> str:
    client_id = settings.COGNITO["app_client_id"]
    client_secret = settings.COGNITO["app_client_secret"]

    msg = username + client_id
    dig = hmac.new(
        client_secret.encode("utf-8"),
        msg=msg.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).digest()
    return base64.b64encode(dig).decode("utf-8")


# =========================
# LOGIN
# =========================
def login_view(request):
    if request.method == "POST":
        email = request.POST.get("email", "").strip()
        password = request.POST.get("password", "").strip()

        if not email or not password:
            messages.error(request, "Please enter email and password.")
            return redirect("login")

        key = f"login:{email}"

        allowed = check_rate_limit(
            key,
            limit=settings.RATE_LIMIT_LOGIN_LIMIT,
            window=settings.RATE_LIMIT_LOGIN_WINDOW,
        )

        if not allowed:
            messages.error(
                request,
                "Too many failed login attempts. Try again in 1 minute."
            )
            return redirect("login")

        client = get_cognito_client()

        try:
            auth_response = client.admin_initiate_auth(
                UserPoolId=settings.COGNITO["user_pool_id"],
                ClientId=settings.COGNITO["app_client_id"],
                AuthFlow="ADMIN_USER_PASSWORD_AUTH",
                AuthParameters={
                    "USERNAME": email,
                    "PASSWORD": password,
                    "SECRET_HASH": get_secret_hash(email),
                },
            )

        except client.exceptions.NotAuthorizedException:
            messages.error(request, "Incorrect email or password.")
            return redirect("login")

        except client.exceptions.UserNotFoundException:
            messages.error(request, "No account found with this email.")
            return redirect("login")

        except client.exceptions.UserNotConfirmedException:
            if getattr(settings, "DEV_MODE", False):
                messages.warning(request, "Email verification skipped (DEV mode).")
                try:
                    client.admin_confirm_sign_up(
                        UserPoolId=settings.COGNITO["user_pool_id"],
                        Username=email,
                    )
                    auth_response = client.admin_initiate_auth(
                        UserPoolId=settings.COGNITO["user_pool_id"],
                        ClientId=settings.COGNITO["app_client_id"],
                        AuthFlow="ADMIN_USER_PASSWORD_AUTH",
                        AuthParameters={
                            "USERNAME": email,
                            "PASSWORD": password,
                            "SECRET_HASH": get_secret_hash(email),
                        },
                    )
                except Exception:
                    messages.error(request, "Incorrect email or password.")
                    return redirect("login")
            else:
                messages.error(request, "Please verify your email before logging in.")
                return redirect("login")

        except ClientError as e:
            messages.error(request, f"Login failed: {e.response['Error']['Message']}")
            return redirect("login")

        except Exception as e:
            messages.error(request, f"Login failed: {str(e)}")
            return redirect("login")

        tokens = auth_response.get("AuthenticationResult", {})
        access_token = tokens.get("AccessToken")
        id_token = tokens.get("IdToken")

        if access_token:
            request.session["cognito_access_token"] = access_token

        if id_token:
            request.session["cognito_id_token"] = id_token

        try:
            user = client.admin_get_user(
                UserPoolId=settings.COGNITO["user_pool_id"],
                Username=email,
            )
        except Exception as e:
            messages.error(request, f"Failed to fetch user info: {e}")
            return redirect("login")

        full_name = None
        email_verified = False
        for attr in user.get("UserAttributes", []):
            if attr["Name"] == "name":
                full_name = attr["Value"]
            elif attr["Name"] == "email_verified":
                email_verified = (attr["Value"] == "true")

        if not getattr(settings, "DEV_MODE", False) and not email_verified:
            messages.error(request, "Please verify your email before logging in.")
            return redirect("login")

        request.session["user_email"] = email
        request.session["user_name"] = full_name
        request.session["user_id"] = email

        groups = get_user_groups(email)
        request.session["cognito_groups"] = groups

        messages.success(request, f"Welcome, {full_name or email}!")
        if "EasyCartAdmins" in groups:
            return redirect("admin_dashboard")

        next_url = request.GET.get("next")
        if next_url:
            return redirect(next_url)

        return redirect("home")

    return render(request, "login.html")


# =========================
# LOGOUT
# =========================
def logout_view(request):
    request.session.flush()
    clear_messages(request)
    messages.success(request, "You have been logged out.")
    return redirect("login")


# =========================
# REGISTER
# =========================
def register(request):
    if request.method == "POST":
        name = request.POST.get("name", "").strip()
        email = request.POST.get("email", "").strip()
        password = request.POST.get("password")

        if not (name and email and password):
            messages.error(request, "Please fill in all fields.")
            return redirect("register")

        client = get_cognito_client()

        try:
            resp = client.sign_up(
                ClientId=settings.COGNITO["app_client_id"],
                SecretHash=get_secret_hash(email),
                Username=email,
                Password=password,
                UserAttributes=[
                    {"Name": "email", "Value": email},
                    {"Name": "name", "Value": name},
                ],
            )

            print("SIGN UP RESP:", resp)

            if getattr(settings, "DEV_MODE", False):
                try:
                    client.admin_confirm_sign_up(
                        UserPoolId=settings.COGNITO["user_pool_id"],
                        Username=email,
                    )
                    messages.success(
                        request,
                        "Account created (DEV MODE auto-confirmed). You can now login."
                    )
                    return redirect("login")
                except Exception as e:
                    messages.warning(request, f"Auto-confirm skipped: {e}")

            request.session["pending_email"] = email
            messages.success(
                request,
                "Account created! We've sent a verification code to your email. "
                "Enter it below to activate your account."
            )
            return redirect("verify_otp")

        except client.exceptions.UsernameExistsException:
            messages.error(request, "This email already exists.")
            return redirect("register")

        except client.exceptions.InvalidPasswordException:
            messages.error(
                request,
                "Password must contain uppercase, lowercase, number, and symbol."
            )
            return redirect("register")

        except ClientError as e:
            messages.error(request, f"Error: {e.response['Error']['Message']}")
            return redirect("register")

        except Exception as e:
            messages.error(request, f"Error: {str(e)}")
            return redirect("register")

    return render(request, "register.html")


# =========================
# VERIFY OTP
# =========================
def verify_otp(request):
    email = request.session.get("pending_email")

    if not email:
        messages.error(request, "No pending registration found. Please register first.")
        return redirect("register")

    client = get_cognito_client()

    if request.method == "POST":
        code = request.POST.get("code", "").strip()

        if not code:
            messages.error(request, "Please enter the verification code.")
            return render(request, "verify_otp.html", {"email": email})

        try:
            client.confirm_sign_up(
                ClientId=settings.COGNITO["app_client_id"],
                SecretHash=get_secret_hash(email),
                Username=email,
                ConfirmationCode=code,
            )

        except client.exceptions.CodeMismatchException:
            messages.error(request, "Invalid verification code. Please try again.")
            return render(request, "verify_otp.html", {"email": email})

        except client.exceptions.ExpiredCodeException:
            messages.error(request, "Verification code expired. Please request a new one.")
            return render(request, "verify_otp.html", {"email": email})

        except ClientError as e:
            messages.error(
                request,
                f"Could not verify your account: {e.response['Error']['Message']}"
            )
            return render(request, "verify_otp.html", {"email": email})

        except Exception:
            messages.error(request, "Could not verify your account. Please try again.")
            return render(request, "verify_otp.html", {"email": email})

        request.session.pop("pending_email", None)
        messages.success(request, "Your email is verified. You can now log in.")
        return redirect("login")

    return render(request, "verify_otp.html", {"email": email})


# =========================
# FORGOT PASSWORD
# =========================
def cognito_forgot_password(username: str):
    client = get_cognito_client()
    try:
        return client.forgot_password(
            ClientId=settings.COGNITO["app_client_id"],
            Username=username,
            SecretHash=get_secret_hash(username),
        )
    except ClientError as e:
        return {"error": e.response["Error"]["Message"]}
    except Exception as e:
        return {"error": str(e)}


def forgot_password(request):
    if request.method == "POST":
        username = request.POST.get("username", "").strip()

        if not username:
            messages.error(request, "Please enter your email.")
            return redirect("forgot_password")

        res = cognito_forgot_password(username)
        if "error" in res:
            messages.error(request, res["error"])
            return redirect("forgot_password")

        request.session["reset_username"] = username
        messages.success(request, "OTP sent to your email.")
        return redirect("reset_password")

    return render(request, "forgot_password.html")


# =========================
# RESET PASSWORD
# =========================
def cognito_confirm_new_password(username: str, code: str, new_password: str):
    client = get_cognito_client()
    try:
        return client.confirm_forgot_password(
            ClientId=settings.COGNITO["app_client_id"],
            Username=username,
            ConfirmationCode=code,
            Password=new_password,
            SecretHash=get_secret_hash(username),
        )
    except ClientError as e:
        return {"error": e.response["Error"]["Message"]}
    except Exception as e:
        return {"error": str(e)}


def reset_password(request):
    username = request.session.get("reset_username")

    if not username:
        messages.error(
            request,
            "No password reset request found. Please request a reset again."
        )
        return redirect("forgot_password")

    if request.method == "POST":
        code = request.POST.get("code", "").strip()
        new_password = request.POST.get("password", "")

        if not code or not new_password:
            messages.error(request, "Please fill in all fields.")
            return redirect("reset_password")

        res = cognito_confirm_new_password(username, code, new_password)

        if "error" in res:
            messages.error(request, res["error"])
            return redirect("reset_password")

        request.session.pop("reset_username", None)
        messages.success(request, "Password reset successful! You can now login.")
        return redirect("login")

    return render(request, "reset_password.html")


# =========================
# PRODUCTS
# =========================
def get_all_categories():
    return ["MenClothes", "WomenClothes", "KidsClothes"]


def generate_presigned_image_url(key: str):
    s3 = boto3.client(
        "s3",
        region_name=settings.S3_REGION,
        config=Config(signature_version="s3v4")
    )

    try:
        return s3.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": settings.S3_BUCKET, "Key": key},
            ExpiresIn=3600,
        )
    except Exception as e:
        print("Error generating image URL:", e)
        return None




def products(request, category=None):
    categories = get_all_categories()
    dynamodb = boto3.resource("dynamodb", region_name=settings.COGNITO["region"])
    items = []

    search = request.GET.get("search", "").strip().lower()

    if search:
        keyword_map = {
            "MenClothes": ["men", "sweater", "hoodie", "polo", "shirt", "tshirt"],
            "WomenClothes": ["women", "top", "dress", "mesh", "striped", "blouse"],
            "KidsClothes": ["kids", "child", "baby", "sweatshirt", "christmas"],
        }

        matched_category = None
        for table_name, words in keyword_map.items():
            if any(w in search for w in words):
                matched_category = table_name
                break

        if matched_category:
            category = matched_category
        else:
            category = None

    try:
        if category:
            if category not in categories:
                messages.error(request, "Invalid category selected.")
                return redirect("products")

            table = dynamodb.Table(category)
            response = table.scan()
            items = response.get("Items", [])

            # ✅ attach category to every item
            for item in items:
                item["category"] = category

        else:
            for cat in categories:
                table = dynamodb.Table(cat)
                response = table.scan()
                cat_items = response.get("Items", [])

                # ✅ attach category to every item
                for item in cat_items:
                    item["category"] = cat

                items.extend(cat_items)

    except ClientError as e:
        messages.error(request, f"DynamoDB error: {e.response['Error']['Message']}")
        return render(request, "products.html", {
            "products": [],
            "category": category,
            "categories": categories,
            "ADD_TO_CART_URL": "",
            "VIEW_CART_URL": "",
            "REMOVE_ITEM_URL": "",
        })

    for item in items:
        key = item.get("image")
        item["image_url"] = generate_presigned_image_url(key) if key else None

    lambda_cfg = settings.COGNITO["lambda_cart_endpoints"]

    return render(request, "products.html", {
        "products": items,
        "category": category,
        "categories": categories,
        "ADD_TO_CART_URL": lambda_cfg["add_to_cart"],
        "VIEW_CART_URL": lambda_cfg["view_cart"],
        "REMOVE_ITEM_URL": lambda_cfg["remove_cart_item"],
    })

# =========================
# CART / CHECKOUT
# =========================
def view_cart(request):
    print("SESSION DATA:", dict(request.session))
    print("USER_ID:", request.session.get("user_id"))
    lambda_cfg = settings.COGNITO["lambda_cart_endpoints"]
    return render(request, "view_cart.html", {
        "VIEW_CART_URL": lambda_cfg["view_cart"],
        "REMOVE_ITEM_URL": lambda_cfg["remove_cart_item"],
        "USER_ID": request.session.get("user_id", ""),
    })

def checkout(request):
    lambda_cfg = settings.COGNITO["lambda_cart_endpoints"]

    return render(request, "checkout.html", {
        "VIEW_CART_URL": lambda_cfg["view_cart"],
        "PLACE_ORDER_URL": lambda_cfg["place_order"],
    })


def order_confirmation(request):
    order_id = request.GET.get("id")
    return render(request, "order_confirmation.html", {
        "order_id": order_id
    })


# =========================
# ADMIN HELPERS
# =========================
def get_user_groups(email):
    client = boto3.client("cognito-idp", region_name=settings.COGNITO["region"])

    resp = client.admin_list_groups_for_user(
        UserPoolId=settings.COGNITO["user_pool_id"],
        Username=email
    )

    return [g["GroupName"] for g in resp.get("Groups", [])]


def admin_required(view_func):
    def wrapper(request, *args, **kwargs):
        groups = request.session.get("cognito_groups", [])

        if "EasyCartAdmins" not in groups:
            messages.error(request, "You are not authorized to access the admin panel.")
            return redirect("home")

        return view_func(request, *args, **kwargs)
    return wrapper


def offers(request):
    return render(request, "offers.html")
    
import requests
from django.http import JsonResponse

def get_location(request):
    try:
        ip = request.META.get('HTTP_X_FORWARDED_FOR', request.META.get('REMOTE_ADDR', ''))
        if ',' in ip:
            ip = ip.split(',')[0].strip()

        res = requests.get(f'https://ipapi.co/{ip}/json/', timeout=5)
        data = res.json()

        return JsonResponse({
            'country_name': data.get('country_name', 'Ireland'),
            'country_code': data.get('country_code', 'IE'),
            'city': data.get('city', ''),
        })
    except Exception:
        return JsonResponse({
            'country_name': 'Ireland',
            'country_code': 'IE',
            'city': '',
        })