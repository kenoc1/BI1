class ProductNameCutter:
    def cutProductNames(self, array):
        products = []
        for i in range (0,len(array)):
            productName = str(array[i][1])
            brand = str(array[i][2])
            productArray = []
            if brand in productName:
                productArray.append(productName.replace(brand, ''))
            else:
                productArray.append(productName)
            productArray.append(array[i][0])
            products.append(productArray)
        return products
            # if name contains marke then remove marke
